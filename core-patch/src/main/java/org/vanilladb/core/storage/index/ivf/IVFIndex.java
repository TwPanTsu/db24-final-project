/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VECTOR;
import static org.vanilladb.core.sql.Type.INTVECTOR;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.sql.Int8VectorType;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.sql.distfn.IntEuclideanFn;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.HashMap;
import org.vanilladb.core.query.algebra.MultiTableScan;
/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class IVFIndex extends Index {

	private static final String centroidTableName = "siftCentroid",
			clusterTableName = "siftCluster",
			centroidTablefldName = "sift_idx";

	private static final int NUM_CLUSTERS;
	private static final int NUM_CLUSTERS_PROBE;

	static {
		NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".NUM_CLUSTERS", 9);
		NUM_CLUSTERS_PROBE = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".NUM_CLUSTERS_PROBE", 9);
	}

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		return 1;
	}

	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *                the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */

	private RecordFile rf;
	private static boolean inited = false;
	private static boolean insertable = false;
	private TableScan selected_ts;
	private static Comparator<siftRecord> comp = new siftRecordComparator();

	/**
	 * Opens a hash index for the specified index.
	 * 
	 * @param ii
	 *                the information of this index
	 * @param keyType
	 *                the type of the search key
	 * @param tx
	 *                the calling transaction
	 */
	public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);

	}

	@Override //優化到可以一次全load進BufferPool時可以用
	public void preLoadToMemory() {
		for (int i = 0; i < NUM_CLUSTERS; i++) {
			String tblname = clusterTableName + "_" + String.valueOf(i) + ".tbl";
			long size = fileSize(tblname);
			BlockId blk;
			for (int j = 0; j < size; j++) {
				blk = new BlockId(tblname, j);
				tx.bufferMgr().pin(blk);
			}
		}
	}

	/**
     * 用 queryVector 找出並打開對應的ClusterTable (assign 到 selected_ts)
	 * 
	 * @see Index#beforeFirst(SearchRange)
	 */
	@Override
	public void beforeFirst(VectorConstant query) {

		close();

		getCentroidTable();

		double min_val = Double.MAX_VALUE;
		int min_idx = -1;
		EuclideanFn distFn = new EuclideanFn("i_imb");
		distFn.setQueryVector(query);

		int idx = 0;

		rf.beforeFirst();
		while (this.rf.next()) {
			VectorConstant centriod = (VectorConstant) this.rf.getVal(centroidTablefldName);
			double dist = distFn.distance(centriod);
			if (dist < min_val) {
				min_val = dist;
				min_idx = idx;
			}
			idx++;
		}

		selected_ts = new TableScan(getClusterTableInfo(min_idx), tx);
		close();
	}


	// 用於 PriorityQueue 的 element 
	static class siftRecord {
		siftRecord(int idx, int dist){
			this.idx = idx;
			this.dist = dist;
		}
		int idx;
		int dist;
	}

	// 用於 PriorityQueue 的 Comparator， dist愈小，優先級越小
	static class siftRecordComparator implements Comparator<siftRecord> {
		public int compare(siftRecord r1, siftRecord r2) {
			int result = Double.compare(r1.dist,r2.dist);
			if (result != 0)
				return -result;
			else
				return 0;
			
		}
	}
	// 和TopKPlan.open()的做法一樣，尋找前NUM_CLUSTERS_PROBE個近的centroid，並回傳對應的NUM_CLUSTERS_PROBE個clusters的table合起來的MultiTableScan()
	public MultiTableScan OpenTopk(Int8VectorConstant query) {
		// CLUSTER_PROBE_NUMS
		int limit = NUM_CLUSTERS_PROBE;
		PriorityQueue<siftRecord> pq = new PriorityQueue<>(limit, (siftRecord r1, siftRecord r2) -> comp.compare(r1, r2));

		close();

		getCentroidTable();

		IntEuclideanFn distFn = new IntEuclideanFn("i_imb");
		distFn.setQueryVector(query);

		int idx = 0;

		rf.beforeFirst();
		while (this.rf.next()) {
			Int8VectorConstant centriod = (Int8VectorConstant) this.rf.getVal(centroidTablefldName);
			int dist = distFn.distance(centriod);

			siftRecord rec = new siftRecord(idx,dist);
			pq.add(rec);
			if (pq.size() > limit)
				pq.poll();
			idx++;
		}

		Object [] recArr = pq.toArray();
		List<TableInfo> ti = new ArrayList<>();
		for(int i = 0; i < limit; i++){
			ti.add(getClusterTableInfo(((siftRecord)recArr[i]).idx));
		}
		close();
		return new MultiTableScan(ti,tx,limit);
	}

	@Override
	public void beforeFirst(SearchRange SearchRange) {
		throw new RuntimeException("cannot go here");
	}

	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		throw new IllegalStateException("You must call beforeFirst() before iterating index '"
				+ ii.indexName() + "'");

	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		throw new RuntimeException("cannot go here");
	}

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
	}

	// 在 benchmark 時， 將 vec 插入對應的 cluster
	// fldValMap 存要 insert 的 record 的資料 (用fldName access)
	public void insertRecord(Map<String, Constant> fldValMap) {

		// 檢查是在 loadtestbed or benchmark (loadtestbed 的話就不要insert)
		if (!inited) {
			inited = true;
			TableInfo ti = getClusterTableInfo(0);

			this.rf = ti.open(tx, false);

			if (rf.fileSize() == 0) {
				insertable = false;
			} else {
				insertable = true;
			}
			return;
		} else if (!insertable) {
			return;
		}

		// 取出要 insert 的 vector 和 id
		VectorConstant vec = (VectorConstant) fldValMap.get("i_emb");
		IntegerConstant id = (IntegerConstant) fldValMap.get("i_id").castTo(INTEGER);
		Int8VectorConstant int8vec = new Int8VectorConstant(vec.asJavaVal());
		// 找到對應的 cluster table (loop through 找最近的)
		getCentroidTable();
		int min_val = Integer.MAX_VALUE;
		int min_idx = -1;
		IntEuclideanFn distFn = new IntEuclideanFn("i_imb");
		distFn.setQueryVector(int8vec);

		int idx = 0;
		rf.beforeFirst();
		while (this.rf.next()) {
			Int8VectorConstant centriod = (Int8VectorConstant) this.rf.getVal(centroidTablefldName);
			int dist = distFn.distance(centriod);
			if (dist < min_val) {
				min_val = dist;
				min_idx = idx;
			}
			idx++;
		}
		close();

		this.rf = getClusterTableInfo(min_idx).open(tx, false);

		// insert 到對應的 cluster table
		rf.insert();
		rf.setVal("i_emb", int8vec);
		rf.setVal("i_id", id);
		close();
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
        return;
		// throw new RuntimeException("public void delete");
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (rf != null)
			rf.close();
	}

	private long fileSize(String fileName) {
		tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}

	public void createCentroidTable(List<Int8VectorConstant> vectors) {

		// this.rf = CentroidTable 的 RecordFile
		getCentroidTable();
		System.out.println(vectors.get(0).toString());
		for (int i = 0; i < vectors.size(); i++) {
			rf.insert();
			rf.setVal(centroidTablefldName, vectors.get(i));
		}

	}

	public void createClusterTable(List<IntegerConstant> ids, List<Int8VectorConstant> vectors, int id) {

		getClusterTable(id);

		for (int i = 0; i < vectors.size(); i++) {
			rf.insert();
			rf.setVal("i_emb", vectors.get(i));
			rf.setVal("i_id", ids.get(i));
		}
		close();
	}

	public TableScan getTableScan() {
		return selected_ts;
	}

	private void getClusterTable(int id) {

		TableInfo ti = getClusterTableInfo(id);
		this.rf = ti.open(tx, false);

		if (rf.fileSize() == 0)
			RecordFile.formatFileHeader(ti.fileName(), tx);
	}

	private TableInfo getClusterTableInfo(int id) {
		String tblname = clusterTableName + "_" + String.valueOf(id);

		Schema sch = new Schema();
		sch.addField("i_id", INTEGER);
		sch.addField("i_emb", INTVECTOR(128));
		TableInfo ti = new TableInfo(tblname, sch);
		return ti;
	}

	private void getCentroidTable() {
		
		String tblname = centroidTableName;
		String fldName = centroidTablefldName;

		Schema sch = new Schema();
		sch.addField(fldName, INTVECTOR(128));
		TableInfo ti = new TableInfo(tblname, sch);
		
		this.rf = ti.open(tx, false);

		if (rf.fileSize() == 0)
			RecordFile.formatFileHeader(ti.fileName(), tx);
	}

}
