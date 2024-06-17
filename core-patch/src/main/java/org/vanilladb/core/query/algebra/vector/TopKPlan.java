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
package org.vanilladb.core.query.algebra.vector;

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.materialize.SortScan;
import org.vanilladb.core.query.algebra.multibuffer.BufferNeeds;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.IntDistanceFn;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.record.RecordFormatter;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The {@link Plan} class for the <em>sort</em> operator.
 */
public class TopKPlan implements Plan {
	private Plan p;
	private Transaction tx;
	private Schema schema;
	private IntDistanceFn distFn;
	private static final int limit = 20;
	// private int NUM_ITEMS = 300;
	private static Comparator<siftRecord> comp = new siftRecordComparator();
	//private ArrayList<siftRecord> siftRecordPool = new ArrayList<siftRecord>(NUM_ITEMS);
	//private siftRecord [] siftRecordPool = new siftRecord[NUM_ITEMS_CONSTANT];
	//private int siftRecordPoolIdx = 0;


	private List<String> sortFlds;
	private List<Integer> sortDirs;

	// 用於 PriorityQueue 的 element 
	static class siftRecord {
		IntegerConstant idx;
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
	// 用於 PriorityQueue 轉成 Scan 的形式，( open() 時會回傳這個 Scan )
	// 會照 Pop() 的順序 return idx。
	static class PriorityQueueScan implements Scan {
        private PriorityQueue<siftRecord> pq;
        private boolean isBeforeFirsted = false;

        public PriorityQueueScan(PriorityQueue<siftRecord> pq) {
            this.pq = pq;
        }

        @Override
        public Constant getVal(String fldName) {
            return pq.peek().idx;
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
            return true;
        }
    }

	public TopKPlan(Plan p, IntDistanceFn distFn, Transaction tx) {
		this.p = p;
		this.sortFlds = new ArrayList<String>();
		this.sortFlds.add(distFn.fieldName());

		this.sortDirs = new ArrayList<Integer>();
		this.sortDirs.add(DIR_ASC);

		this.tx = tx;
		this.schema = p.schema();

		this.distFn = distFn;

	}

	/**
	 * 對ChildScan找TopK個element。
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		PriorityQueue<siftRecord> pq = new PriorityQueue<>(limit, (siftRecord r1, siftRecord r2) -> comp.compare(r1, r2));

		// p 是 childPlan (IndexSelectVecPlan)
		// p.open() 會回傳 query vector 對應到的 Cluster 的 TableScan

		Scan src = p.open();

		src.beforeFirst();
		while (src.next()) {
			siftRecord rec = new siftRecord();

			// 紀錄該 record 的idx和它與query vector的距離。
			rec.dist = distFn.distance((Int8VectorConstant) src.getVal(sortFlds.get(0)));
			rec.idx = (IntegerConstant) src.getVal("i_id");

			// 將 rec 插到 PriorityQueue，pq element的個數超過20個就pop()，
			// 因為是dist越大的排越前面，所以做後只會剩dist最小的20個在pq裡面
			// 也就是最近的前20個vector
			pq.add(rec);
			if (pq.size() > limit)
				pq.poll();
		}
        if(pq.size() < limit){
            Set<Integer> nearestNeighbors = new HashSet<>();
            Object[] recArr = (Object[])pq.toArray();
            for(Object rec : recArr){
                nearestNeighbors.add((Integer)((siftRecord)rec).idx.asJavaVal());
            }
            int idx = 0;
            while (pq.size() < limit) {
                // pq沒滿20個的話補到20個，不然會報錯。
                if(nearestNeighbors.contains(idx)){
                    idx++;
                    continue;
                }
                siftRecord rec = new siftRecord();
                rec.dist = idx;
                rec.idx = new IntegerConstant(idx);
                pq.add(rec);
                idx++;
            }
        }

		
		src.close();
		return new PriorityQueueScan(pq);
	}

	/**
	 * Returns the number of blocks in the sorted table, which is the same as it
	 * would be in a materialized table. It does <em>not</em> include the
	 * one-time cost of materializing and sorting the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the sorted table, which is the same as in the
	 * underlying query.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return p.recordsOutput();
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : schema.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

	@Override
	public String toString() {
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("TopKPlan (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
