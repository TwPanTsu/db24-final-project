/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.bench.server.procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.util.RandomNonRepeatGenerator;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorType;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.buffer.EmptyPageFormatter;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.ivf.IVFSq8DirectIndex;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class StoredProcedureUtils {
	
	public static Scan executeQuery(String sql, Transaction tx) {
		Plan p = VanillaDb.newPlanner().createQueryPlan(sql, tx);
		return p.open();
	}
	
	public static int executeUpdate(String sql, Transaction tx) {
		return VanillaDb.newPlanner().executeUpdate(sql, tx);
	}

	public static void executeTrainIndex(String tableName, List<String> idxFields, String idxName, Transaction tx) {
		// Obtain metadata about the index to be trained
		IVFSq8DirectIndex idx = (IVFSq8DirectIndex)VanillaDb.catalogMgr().getIndexInfoByName(idxName, tx).open(tx);
		String field = idxFields.get(0);
		// Throws an exception if the type cast fails
		Type idxType = (VectorType)VanillaDb.catalogMgr().getTableInfo(tableName, tx).schema().type(field);
		int dim = idxType.getArgument();

		for (int i = 0; i < IVFSq8DirectIndex.numCentroidBlocks(new SearchKeyType(idxType)); ++i)
			tx.bufferMgr().pinNew(idx.centroidName(), new EmptyPageFormatter());

		//Initialize centroids------------------------------------------------------------------------
		RandomNonRepeatGenerator RNRG = new RandomNonRepeatGenerator(SiftBenchConstants.NUM_ITEMS);
		Map<Integer, Integer> M = new HashMap<>();
		for (int i = 0; i < IVFSq8DirectIndex.NUM_CENTROIDS; ++i){
			int random_number = RNRG.next();
			M.put(random_number,i);
		}
		
		Plan test_tp = new TablePlan(tableName, tx);
		Scan test_ts = test_tp.open();
		test_ts.beforeFirst();
		while(test_ts.next()){
			int index = (Integer)test_ts.getVal("i_id").asJavaVal();
			if(M.containsKey(index)){
				VectorConstant v = new VectorConstant((float[])test_ts.getVal("i_emb").asJavaVal());
				idx.setCentroidVector(M.get(index), v);
			}
		}
		test_ts.close();
		//----------------------------------------------------------------------------------------------

		//Refine centroids------------------------------------------------------------------------------
		int iteration = 1;

		for (int i = 0; i < iteration; i++){
			System.err.print("Iteration " + i + "\n");
			Plan tp = new TablePlan(tableName, tx);
			Scan ts = tp.open();
			
			VectorConstant[] sum = new VectorConstant[IVFSq8DirectIndex.NUM_CENTROIDS];
			for (int j = 0; j < IVFSq8DirectIndex.NUM_CENTROIDS; j++){
				sum[j] =  VectorConstant.zeros(dim);
			}

			int [] num_of_points = new int[IVFSq8DirectIndex.NUM_CENTROIDS];

			//cluster the data points
			ts.beforeFirst();
			while(ts.next()){
				VectorConstant temp;	// a copy of a record of the table

				// Two field:
				// 1: i_emb(VectorConstant) 
				// 2: i_id(IntegerConstant)

				temp = (VectorConstant)ts.getVal("i_emb");

				float min_dist = Float.MAX_VALUE;
				int belongsTo = 0;

				EuclideanFn fn = new EuclideanFn("i_emb"); //i don't know wtf
				fn.setQueryVector(temp);
				for (int j  = 0; j < IVFSq8DirectIndex.NUM_CENTROIDS; j++){
					VectorConstant center = idx.getCentroidVector(j);
					float distance = (float)fn.distance(center);
 					if( distance < min_dist){
						min_dist = distance;
						belongsTo = j;
					}
				}

				num_of_points[belongsTo]++;
				sum[belongsTo] = (VectorConstant)sum[belongsTo].add(temp);

				//Update centroid (Mini-batch kmeans)
				if (((Integer)ts.getVal("i_id").asJavaVal()) % 10000 == 0){
					VectorConstant[] avg = new VectorConstant[IVFSq8DirectIndex.NUM_CENTROIDS];
					for(int j = 0; j < IVFSq8DirectIndex.NUM_CENTROIDS; j++){
						System.err.print("number of points: " + num_of_points[j] + " in" + " cluster " + j + "\n");
						avg[j] = (VectorConstant)sum[j].div_by_int(num_of_points[j]);
					}
					for(int j = 0; j < IVFSq8DirectIndex.NUM_CENTROIDS; j++){
						if(num_of_points[j] != 0){
							idx.setCentroidVector(j, avg[j]);
						}
					}
				}

			}

			// Update centroid
			// for(int j = 0; j < IVFSq8DirectIndex.NUM_CENTROIDS; j++){
			// 	// System.err.print("number of points: " + num_of_points[j] + " in" + " cluster " + j + "\n");
			// 	sum[j] = (VectorConstant)sum[j].div_by_int(num_of_points[j]);
			// }
			// for(int j = 0; j < IVFSq8DirectIndex.NUM_CENTROIDS; j++){
			// 	idx.setCentroidVector(j, sum[j]);
			// }
			
			ts.close();
		}
		//----------------------------------------------------------------------------------------------


		// Build the index
		// Write the records into their corresponding clusters (tables)
		Plan p = new TablePlan(tableName, tx);
		UpdateScan s = (UpdateScan)p.open();
		s.beforeFirst();
		// int i = 0;
		// System.err.print(i + "/900000\r");
		while (s.next()) {
			// Construct a map from field names to values
			Map<String, Constant> fldValMap = new HashMap<String, Constant>();
			for (String fldname : p.schema().fields())
				fldValMap.put(fldname, s.getVal(fldname));
			RecordId rid = s.getRecordId();

			idx.insert(new SearchKey(p.schema().fields(), fldValMap), rid, false);
			// if (++i % 900 == 0)
			// 	System.err.print(i + "/900000\r");
		}
		s.close();
	}

	static class MapRecord implements Record{

		Map<String, Constant> fldVals = new HashMap<>();

		@Override
		public Constant getVal(String fldName) {
			return fldVals.get(fldName);
		}

		public void put(String fldName, Constant val) {
			fldVals.put(fldName, val);
		}

		public boolean containsKey(String fldName) {
			return fldVals.containsKey(fldName);
		}
	}

	static class PriorityQueueScan implements Scan {
        private PriorityQueue<MapRecord> pq;
        private boolean isBeforeFirsted = false;

        public PriorityQueueScan(PriorityQueue<MapRecord> pq) {
            this.pq = pq;
        }

        @Override
        public Constant getVal(String fldName) {
            return pq.peek().getVal(fldName);
        }

        @Override
        public void beforeFirst() {
            this.isBeforeFirsted = true;
        }

        @Override
        public boolean next() {
            if (isBeforeFirsted) {
                isBeforeFirsted = false;
                return true;
            }
            pq.poll();
            return pq.size() > 0;
        }

        @Override
        public void close() {
            return;
        }

        @Override
        public boolean hasField(String fldName) {
            return pq.peek().containsKey(fldName);
        }
    }

	public static Scan executeCalculateRecall(VectorConstant query, String tableName, String field, int limit, Transaction tx) {
		Plan p = new TablePlan(tableName, tx);

		DistanceFn distFn = new EuclideanFn(field);
		distFn.setQueryVector(query);

		List<String> sortFlds = new ArrayList<String>();
		sortFlds.add(distFn.fieldName());
		
		List<Integer> sortDirs = new ArrayList<Integer>();
		sortDirs.add(DIR_DESC); // for priority queue

		RecordComparator comp = new RecordComparator(sortFlds, sortDirs, distFn);

		PriorityQueue<MapRecord> pq = new PriorityQueue<>(limit, (MapRecord r1, MapRecord r2) -> comp.compare(r1, r2));
		
		Scan s = p.open();
		s.beforeFirst();
		while (s.next()) {
			MapRecord fldVals = new MapRecord();
			for (String fldName : p.schema().fields()) {
				fldVals.put(fldName, s.getVal(fldName));
			}
			pq.add(fldVals);
			if (pq.size() > limit)
				pq.poll();
		}
		s.close();

		s = new PriorityQueueScan(pq);

		return s;
	}

	public static int executeInsert(InsertData sql, Transaction tx) {
		return VanillaDb.newPlanner().executeInsert(sql, tx);
	}
}
