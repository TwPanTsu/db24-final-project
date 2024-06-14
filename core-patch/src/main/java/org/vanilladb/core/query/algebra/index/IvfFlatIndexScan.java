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
package org.vanilladb.core.query.algebra.index;

import java.util.ArrayList;
import java.util.PriorityQueue;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.index.ivfflat.IVFFlatIndex;
import org.vanilladb.core.storage.index.ivfflat.VectorPairComparable;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.util.CoreProperties;

/**
 * The scan class corresponding to the select relational algebra operator.
 */
public class IvfFlatIndexScan implements UpdateScan {
    private IVFFlatIndex idx;
    private TableScan ts;
    private DistanceFn distFn;
    private PriorityQueue<VectorPairComparable> pq;
    private boolean start;

    public static final int NUM_CLUSTERS_MAX = CoreProperties.getLoader().getPropertyAsInteger(
            IVFFlatIndex.class.getName() + ".NUM_CLUSTERS_MAX", 10);

    /**
     * Creates an index select scan for the specified index and search range.
     * 
     * @param idx
     *               the index
     * @param distFn
     *               the range of search keys
     * @param ts
     *               the table scan of data table
     */
    public IvfFlatIndexScan(Index idx, DistanceFn distFn, TableScan ts) {
        this.idx = (IVFFlatIndex) idx;
        this.distFn = distFn;
        this.ts = ts;
    }

    /**
     * Positions the scan before the first record, which in this case means
     * positioning the index before the first instance of the selection
     * constant.
     * 
     * @see Scan#beforeFirst()
     */
    @Override
    public void beforeFirst() {
        idx.beforeFirst(this.distFn);
        var new_pq = new PriorityQueue<VectorPairComparable>((a, b) -> b.compareTo(a));

        while (idx.next()) {
            var vr = idx.getVectorPair();
            if (new_pq.size() == NUM_CLUSTERS_MAX)
                new_pq.poll();
            new_pq.add(new VectorPairComparable(vr.VectorConst, vr.rid, distFn));
        }

        pq = new PriorityQueue<>(new ArrayList<>(new_pq));
        start = false;
        idx.close();
    }

    /**
     * Moves to the next record, which in this case means moving the index to
     * the next record satisfying the selection constant, and returning false if
     * there are no more such index records. If there is a next record, the
     * method moves the tablescan to the corresponding data record.
     * 
     * @see Scan#next()
     */
    @Override
    public boolean next() {
        if (!start) {
			start = true;
			if (pq.isEmpty())
				return false;
			return true;
		}
		pq.poll();
		return !pq.isEmpty();
    }

    /**
     * Closes the scan by closing the index and the tablescan.
     * 
     * @see Scan#close()
     */
    @Override
    public void close() {
        idx.close();
        ts.close();
        pq.clear();
    }

    /**
     * Returns the value of the field of the current data record.
     * 
     * @see Scan#getVal(java.lang.String)
     */
    @Override
    public Constant getVal(String fldName) {
        var vec = pq.peek();
		ts.moveToRecordId(vec.rid);
        return ts.getVal(fldName);
    }

    /**
     * Returns whether the data record has the specified field.
     * 
     * @see Scan#hasField(java.lang.String)
     */
    @Override
    public boolean hasField(String fldName) {
        return ts.hasField(fldName);
    }

    @Override
    public void setVal(String fldName, Constant val) {
        ts.setVal(fldName, val);
    }

    @Override
    public void delete() {
        ts.delete();
    }

    @Override
    public void insert() {
        ts.insert();
    }

    @Override
    public RecordId getRecordId() {
        return ts.getRecordId();
    }

    @Override
    public void moveToRecordId(RecordId rid) {
        ts.moveToRecordId(rid);
    }
}
