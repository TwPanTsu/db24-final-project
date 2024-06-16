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

import java.sql.Types;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VectorConstant;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.sql.distfn.EuclideanFn;

/**
 * A Ivfflat index implementation of {@link Index}. the 0th indexname is the centroid files
 * the [1, n + 1] is the data files, where n is the number of clusters 
 */
public class IvfIndex extends Index {
	
	/**
	 * A field name of the schema of index records.
	 */
	private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id";

//	public static final int NUM_BUCKETS;
//
//	static {
//		NUM_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
//				IVFIndex.class.getName() + ".NUM_BUCKETS", 100);
//	}

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		return 0;
	}
	
	private static String keyFieldName(int index) {
		return SCHEMA_KEY + index;
	}
	
	private static final int NUM_CLUSTERS;
	static {
		NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(
				IvfIndex.class.getName() + ".NUM_CLUSTERS", 100);
	}

		
	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema(SearchKeyType keyType) {
		Schema sch = new Schema();
		for (int i = 0; i < keyType.length(); i++)
			sch.addField(keyFieldName(i), keyType.get(i));
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}
	
	private SearchKey searchKey;
	private RecordFile rf, rfCentroid;
	private boolean isBeforeFirsted;
	private int currentNearestCentroids;
	private int vecDim;
	
    
	@Override
	public void preLoadToMemory() {

		for (int i = 0; i < (NUM_CLUSTERS + 1); i++) {
			String tblname = ii.indexName() + i + ".tbl";
			long size = fileSize(tblname);
			BlockId blk;
			for (int j = 0; j < size; j++) {
				blk = new BlockId(tblname, j);
				tx.bufferMgr().pin(blk);
			}
		}
	}
	/**
	 * Opens an IVFIndex for the specified index. Create a new record file 
	 * for the centroid
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the search key
	 * @param tx
	 *            the calling transaction
	 */
	public IvfIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
		// get vector dimension 
		vecDim = (keyType.get(keyType.getVecIdx())).getArgument();
		String tblname = ii.indexName() + 0;
		TableInfo ti = new TableInfo(tblname, schema(keyType));
		this.rfCentroid = ti.open(tx, false);
		
		if (rfCentroid.fileSize() == 0) 
			RecordFile.formatFileHeader(ti.fileName(), tx);
		rfCentroid.beforeFirst();
		for (int m = 0; m < NUM_CLUSTERS; ++m) {
			rfCentroid.insert();
			for (int i = 0; i < keyType.length(); ++i) {
				rfCentroid.setVal(keyFieldName(i), VectorConstant.zeros(vecDim));
			}
			rfCentroid.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(0));
			rfCentroid.setVal(SCHEMA_RID_ID, new IntegerConstant(m + 1));
		}
		rfCentroid.close();
		
		
		tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), null, 0, 0);
		
	}


	
	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a {@link RecordFile} on the file corresponding to the bucket.
	 * The record file for the previous bucket (if any) is closed.
	 * 
	 * @see Index#beforeFirst(SearchRange)
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {

		close();
		// support the equality query only
		if (!searchRange.isSingleValue())
			throw new UnsupportedOperationException();

		this.searchKey = searchRange.asSearchKey();
		
		currentNearestCentroids = getNearestCentroids(searchKey);
		
		String tblname = ii.indexName() + currentNearestCentroids; 
		
		TableInfo ti = new TableInfo(tblname, schema(keyType));

		this.rf = ti.open(tx, false);

		if (rf.fileSize() == 0) {
			RecordFile.formatFileHeader(ti.fileName(), tx);
			rfCentroid.beforeFirst();
			while(rfCentroid.next()) {
				Integer currentCentroidId = (Integer) rfCentroid.getVal(SCHEMA_RID_ID).asJavaVal();
				if (currentCentroidId == currentNearestCentroids) {
					rfCentroid.setVal(keyFieldName(0), searchKey.get(0));
				}
			}
		}
		rf.beforeFirst();
		
		isBeforeFirsted = true;
	}
	
	private int getNearestCentroids(SearchKey searchKey) {
		if (this.rfCentroid == null) {
			String tblname = ii.indexName() + 0;
			TableInfo ti = new TableInfo(tblname, schema(keyType));
			this.rfCentroid = ti.open(tx, false);
			if (rfCentroid.fileSize() == 0)
				RecordFile.formatFileHeader(ti.fileName(), tx);
		}

		rfCentroid.beforeFirst();
	    
		int near_id = -1;
		double min_dist = 999999999;
		while (rfCentroid.next()) {
			Constant CentroidId = rfCentroid.getVal(SCHEMA_RID_ID);
			Integer centroidId = (Integer) CentroidId.asJavaVal();
			EuclideanFn eu = new EuclideanFn(SCHEMA_RID_ID);
			VectorConstant QueryVector = null;
			VectorConstant vecVector = null;
			for(int i = 0; i < searchKey.length(); i++) {
				if (searchKey.get(i).getType().getSqlType() == Types.ARRAY) QueryVector = (VectorConstant) searchKey.get(i);
			}
			SearchKey vec = getKey(rfCentroid);
			for(int i = 0; i < searchKey.length(); i++) {
				if (vec.get(i).getType().getSqlType() == Types.ARRAY) vecVector = (VectorConstant) vec.get(i);
			}

			if(QueryVector == null || vecVector == null)
				throw new IllegalArgumentException("Both should have a VectorConstant");
			eu.setQueryVector(QueryVector);
			double dist = eu.calculateDistance(vecVector);

			if (min_dist >= dist){
				min_dist = dist;
				near_id = centroidId;
			}
			
		}
		return near_id;
	}
	

	
	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index '"
					+ ii.indexName() + "'");
		
		while (rf.next())
			if (getKey().equals(searchKey))
					return true;
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}
	

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
		beforeFirst(new SearchRange(key));
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// insert the data
		rf.insert();
		for (int i = 0; i < keyType.length(); i++)
			rf.setVal(keyFieldName(i), key.get(i));
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
				.number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// search the position
		beforeFirst(new SearchRange(key));
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				break;
			}
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
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
		if (rfCentroid != null) {
			rfCentroid.close();
		}
	}
	private long fileSize(String fileName) {
		tx.concurrencyMgr().readFile(fileName);
		return VanillaDb.fileMgr().size(fileName);
	}
	private SearchKey getKey() {
		Constant[] vals = new Constant[keyType.length()];
		for (int i = 0; i < vals.length; i++)
			vals[i] = rf.getVal(keyFieldName(i));
		return new SearchKey(vals);
	}
	private SearchKey getKey(RecordFile recordFile) {
		Constant[] vals = new Constant[keyType.length()];
		for (int i = 0; i < vals.length; i++)
			vals[i] = recordFile.getVal(keyFieldName(i));
		return new SearchKey(vals);
	}
}
	
