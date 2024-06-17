// MIKE: Implement the IVFIndex class
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

import java.util.ArrayList;
import java.util.List;


import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VECTOR;

import org.vanilladb.core.query.planner.opt.AdvancedQueryPlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
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

import smile.math.distance.EuclideanDistance;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.EuclideanFn;

/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class IVFIndex extends Index {
	
	/**
	 * A field name of the schema of index records.
	 */

	public static final String SCHEMA_ID = "i_id", SCHEMA_VECTOR = "i_emb", SCHEMA_COUNT = "i_count";
	public static final String CENTROID_NAME = "_centroid";
	public static final String CLUSTER_COUNT = "_count";
	public static final String INDEXNAME = "idx_sift";
	
	public static final int N = CoreProperties.getLoader().getPropertyAsInteger(AdvancedQueryPlanner.class.getName() + ".N", 10);
    public static final int K = CoreProperties.getLoader().getPropertyAsInteger(AdvancedQueryPlanner.class.getName() + ".K", 50);
	
	public static final int DIMENSION = CoreProperties.getLoader().getPropertyAsInteger(KmeansAlgo.class.getName() + ".DIMENSION", 128);
    public static final int random_seed = CoreProperties.getLoader().getPropertyAsInteger(KmeansAlgo.class.getName() + ".SEED", 56);


	/**
	 * Returns the schema of the index records.
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema() {
		Schema sch = new Schema();
		sch.addField(SCHEMA_ID, INTEGER);
		sch.addField(SCHEMA_VECTOR, VECTOR(128));
		return sch;
	}

	private static Schema schemaCentroid() {
		Schema sch = new Schema();
		sch.addField(SCHEMA_VECTOR, VECTOR(128));
		return sch;
	}

	private static Schema schemaCount() {
		Schema sch = new Schema();
		sch.addField(SCHEMA_COUNT, INTEGER);
		return sch;
	}
	
	private int cur_centroid_id = 0;
	private static List<float[]> centroidVecList = null;
	private static List<SearchKey> recordList = new ArrayList<SearchKey>(100000);
	public  static  int[] cluster_count = new int[K];

	public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
	}

	
	@Override
	public void preLoadToMemory() {
		// NOTE: not implemented yet, we can think of what case will this function be used and where to use
		// System.out.println("Preloading to memory...");
		if(centroidVecList == null) {
			// load the centroid page
			centroidVecList = new ArrayList<float[]>();
			TableInfo ti = new TableInfo(ii.indexName() + CENTROID_NAME, schemaCentroid());
			RecordFile rf = ti.open(tx, false);
			rf.beforeFirst();
			while (rf.next()) {
				// System.out.println("Loading centroid...");
				float[] vec = (float [])rf.getVal(SCHEMA_VECTOR).asJavaVal();
				centroidVecList.add(vec);
			}
			rf.close();
		}

		TableInfo tiCount = new TableInfo(ii.indexName()+CLUSTER_COUNT, schemaCount());
		RecordFile rfCount = tiCount.open(tx, true);
		rfCount.beforeFirst();
		int c=0;
		while (rfCount.next()) {
			cluster_count[c] = (int)rfCount.getVal(SCHEMA_COUNT).asJavaVal();
			System.out.println("cluster " + c + " has " + cluster_count[c] + " records");
			c++;
		}
		rfCount.close();
		
	}

	@Override
	public void beforeFirst(SearchRange searchRange) {
		if (centroidVecList == null) {
			// open the record file
			preLoadToMemory();
			cur_centroid_id = -1;
		} else {
			cur_centroid_id = -1;
		}
	}
	@Override
	public boolean next() {
	
		if (cur_centroid_id + 1 >= centroidVecList.size()) {
			return false;
		}
		else {
			cur_centroid_id++;
			return true;
		}
	}

	@Override
	public RecordId getDataRecordId() {
		// NOTE: This is a dummy function since we don't have data record id in IVF index

		return new RecordId(null, 0);
	}

	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		int clusterId = chooseInsertCluster(key);
		cluster_count[clusterId]++;
		// System.out.println("Inserting record into cluster " + clusterId);

		// open the record file
		TableInfo ti = new TableInfo(ii.indexName() + clusterId, schema());
		RecordFile rf = ti.open(tx, true);
		if (rf.fileSize() == 0)
			RecordFile.formatFileHeader(ti.fileName(), tx);
		rf.beforeFirst();
		rf.insert();
		rf.setVal(SCHEMA_ID, key.get(0));
		rf.setVal(SCHEMA_VECTOR, key.get(1));
		rf.close();
	}

	public void load(SearchKey key) {
		// NOTE: insert data into static list to be used later in clustering

		recordList.add(key);
	}

	
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// NOTE: not implemented yet
	}

	@Override
	public void close() {
		// NOTE: close the corresponding record file that was opened in beforeFirst()
		cur_centroid_id = -1;
	}

	public void train(Transaction tx) {
		// obtain lists of clustered records
		List<List<SearchKey>> clusters = clustering();
		System.out.println("clusters num: " + clusters.size());

		// create centroid page 
		System.out.println("Creating centroid page...");
		String tblName = ii.indexName() + CENTROID_NAME;
		TableInfo tiCentroid = new TableInfo(tblName, schemaCentroid());
		VanillaDb.catalogMgr().createTable(tblName, schemaCentroid(), tx);
		RecordFile rfCentroid = tiCentroid.open(tx, true);
		if (rfCentroid.fileSize() == 0)
			RecordFile.formatFileHeader(tiCentroid.fileName(), tx);
		rfCentroid.beforeFirst();
		for (float[] vec : centroidVecList) {
			rfCentroid.insert();
			rfCentroid.setVal(SCHEMA_VECTOR, new VectorConstant(vec));
		}
		rfCentroid.close();
		int c=0;
		// create data page
		for (int i = 0; i < clusters.size(); i++) {
			// create a table file for each cluster
			String tblname = ii.indexName() + i;
			TableInfo ti = new TableInfo(tblname, schema());
			VanillaDb.catalogMgr().createTable(tblname, schema(), tx);
			RecordFile rf = ti.open(tx, true);

			// initialize the file header if needed
			if (rf.fileSize() == 0)
				RecordFile.formatFileHeader(ti.fileName(), tx);
			rf.beforeFirst();

			List<SearchKey> cluster = clusters.get(i);
			cluster_count[i] = cluster.size();
			c+=cluster.size();
			// insert records
			System.out.println("insert " + cluster.size() + " records in cluster " + i);
			// System.out.println("cluster size: " + cluster_count[i]);
			for (SearchKey record : cluster) {
				rf.insert();
				// System.out.println(record.get(0));
				// System.out.println(record.get(1));
				// System.out.println("record length: " + record.length());
				// System.out.println("id is: " + record.get(0));
				rf.setVal(SCHEMA_ID, record.get(0));
				rf.setVal(SCHEMA_VECTOR, record.get(1));
			}

			// close the index files
			rf.close();
		}
		System.out.println("total records: " + c);
		System.out.println("Creating CLUSTER_COUNT...");
		tblName = ii.indexName() + CLUSTER_COUNT;
		TableInfo tiCount = new TableInfo(tblName, schemaCount());
		VanillaDb.catalogMgr().createTable(tblName, schemaCount(), tx);
		RecordFile rfCount = tiCount.open(tx, true);
		if (rfCount.fileSize() == 0)
			RecordFile.formatFileHeader(tiCentroid.fileName(), tx);
			rfCount.beforeFirst();
		for (int cnt : cluster_count) {
			rfCount.insert();
			rfCount.setVal(SCHEMA_COUNT, new IntegerConstant(cnt));
		}
		rfCount.close();
		
	}

	public Constant getval(String fldName) {
		if (cur_centroid_id >= centroidVecList.size()) {
			System.err.println("Error: no more records");
			return null;
		}
			
		return new VectorConstant(centroidVecList.get(cur_centroid_id));
	}

	public  int getClusterCount(int id) {
        return cluster_count[id];
    }

	private List<List<SearchKey>> clustering() {
		// NOTE: clustering algorithm
		//		 return the List of List cluster records
		// 		 	* outer List -> clusters
		// 			* inner List -> records in a cluster

		// Step1: input training data
		System.out.println("Initializing IndexCluster...\n");
		KmeansAlgo indexCluster = new KmeansAlgo(recordList);

		// Step2: start training
		System.out.println("Training IndexCluster...\n");
		indexCluster.KmeansTraining();

		// Step3: format the output data
		centroidVecList = indexCluster.getCentroidList();
		return indexCluster.getClusteringList();

	}

	private int chooseInsertCluster(SearchKey key) {
		int min_id = 0;
		int min_dist = Integer.MAX_VALUE;
		float[] keyVec = (float[]) key.get(1).asJavaVal();
		EuclideanFn distance = new EuclideanFn("");
		distance.setQueryVector(new VectorConstant(keyVec));
		for (float[] vec : centroidVecList) {
			int dist = (int) distance.distance(new VectorConstant(vec));
			if (dist < min_dist) {
				min_dist = dist;
				min_id = centroidVecList.indexOf(vec);
			}
		}
		return min_id;
	}
}
