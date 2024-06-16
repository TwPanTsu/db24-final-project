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
package org.vanilladb.core.storage.index.ivfflat;


import java.util.ArrayList;
import java.util.List;

import java.util.logging.Logger;


import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
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
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.util.CoreProperties;


/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class IVFFlatIndex extends Index {
    // TODO: rename
    // * implement IVF Flat Clustering
    /**
     * A field name of the schema of index records.
     */
    private static Logger logger = Logger.getLogger(IVFFlatIndex.class.getName());
    // private static final String SCHEMA_CLUSTER_ID = "cid", SCHEMA_KEY = "key";
    // private static final String SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id"; // block and id to the original table
    // private static final String SIFT_TABLE = "sift";
    // private static final String SIFT_EMB = "i_emb";

    public static final int NUM_CLUSTERS_MAX;
    public static final int NUM_CLUSTERS_SIZE;
    public static final int NUM_DIMENSION;

    private ConcurrencyMgr ccMgr;
    private IndexInfo ii;
    private TableInfo ti;

    private static int clusterNum = 0;
    private int targetCluster = 0;
    private static int numNeighbors = 20; // topk

    private RecordId dataRecordId;

    private List<Integer> matched = new ArrayList<>();
    private static List<Cluster> listCluster = new ArrayList<>();

    // private static int numCluster = 20;

    // private List<VectorPair> listCentroids = null;

    static {
        NUM_CLUSTERS_MAX = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_CLUSTERS_MAX", 30);
        NUM_CLUSTERS_SIZE = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_CLUSTERS_SIZE", 300);
        NUM_DIMENSION = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_DIMENSION", 128);

    }
    // true: bench, false: load testbed
    private final boolean isBench = true;

    public IVFFlatIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
        this.ccMgr = tx.concurrencyMgr();
        this.ii = ii;

        // this.ti = getIndexUsedTableInfo(ii.indexName(), schema(keyType));
        if (isBench)
            for (int i = 0; i < NUM_CLUSTERS_MAX; i++) {
                Cluster cluster = new Cluster(i, tx);
                listCluster.add(cluster);
            }

    }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::searchCost'");
    }

    private static String keyFieldName(int index) {
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::keyFieldName'");
        // return SCHEMA_KEY + index;
    }

    /**
     * Returns the schema of the index records.
     * 
     * @param fldType
     *                the type of the indexed field
     * 
     * @return the schema of the index records
     */
    // private static Schema schema(SearchKeyType keyType) {
    //     Schema sch = new Schema();
    //     // for (int i = 0; i < keyType.length(); i++)
    //     sch.addField(SCHEMA_KEY, keyType.get(0));
    //     sch.addField(SCHEMA_RID_BLOCK, BIGINT);
    //     sch.addField(SCHEMA_RID_ID, INTEGER);
    //     sch.addField(SCHEMA_CLUSTER_ID, INTEGER);
    //     return sch;
    // }

    private DistanceFn distFn;
    private RecordFile rf;
    private boolean isBeforeFirsted;

    @Override
    public void preLoadToMemory() {
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::preLoadToMemory'");
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
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::beforeFirst'");
    }

    public void beforeFirst(DistanceFn distFn) {
        this.distFn = distFn;

        targetCluster = 0;
        Double distance = Double.MAX_VALUE;
        for (int i = 0; i < NUM_CLUSTERS_MAX; i++) {
            VectorConstant centroid = listCluster.get(i).getCentroid();
            double curDistance = distFn.distance(centroid);
            if (curDistance < distance) {
                distance = curDistance;
                targetCluster = i;
            }
        }

        isBeforeFirsted = true;
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
        if (matched.size() > numNeighbors)
            return false;

        int target = 0;
        Double distance = Double.MAX_VALUE;
        for (int i = 0; i < listCluster.get(targetCluster).size(); i++) {
            if (matched.contains(i))
                continue;
            VectorConstant centroid = listCluster.get(targetCluster).getVecs().get(i).VectorConst;
            double curDistance = distFn.distance(centroid);
            if (curDistance < distance) {
                distance = curDistance;
                target = i;
            }
        }
        matched.add(target);
        dataRecordId = new RecordId(
                new BlockId(dataFileName, listCluster.get(targetCluster).getVecs().get(target).blk),
                listCluster.get(targetCluster).getVecs().get(target).rid);
        return true;
    }

    /**
     * Retrieves the data record ID from the current index record.
     * 
     * @see Index#getDataRecordId()
     */
    @Override
    public RecordId getDataRecordId() {
        return dataRecordId;
    }

    /**
     * Inserts a new index record into this index.
     * 
     * @see Index#insert(SearchKey, RecordId, boolean)
     */
    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {

        VectorConstant vec = (VectorConstant) key.get(0);

        if (clusterNum < NUM_CLUSTERS_MAX) {
            Cluster cluster = new Cluster(dataRecordId, vec, clusterNum, tx);
            listCluster.add(cluster);
            clusterNum += 1;
        } else {
            int targetCluster = 0;
            Double distance = Double.MAX_VALUE;
            for (int i = 0; i < clusterNum; i++) {
                if (listCluster.get(i).size() >= NUM_CLUSTERS_SIZE)
                    continue;
                VectorConstant centroid = listCluster.get(i).getCentroid();
                EuclideanFn distFn = new EuclideanFn(centroid);
                double curDistance = distFn.distance(vec);
                if (curDistance < distance) {
                    distance = curDistance;
                    targetCluster = i;
                }
            }
            listCluster.get(targetCluster).addVec(vec, dataRecordId, tx);
        }
    }

    /**
     * Deletes the specified index record.
     * 
     * @see Index#delete(SearchKey, RecordId, boolean)
     */
    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::delete'");
        // search the position
        // beforeFirst(new SearchRange(key));

        // // log the logical operation starts
        // if (doLogicalLogging)
        // tx.recoveryMgr().logLogicalStart();

        // // delete the specified entry
        // while (next())
        // if (getDataRecordId().equals(dataRecordId)) {
        // rf.delete();
        // return;
        // }

        // // log the logical operation ends
        // if (doLogicalLogging)
        // tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
        // dataRecordId.block().number(), dataRecordId.id());
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
        if (matched != null)
            matched.clear();
    }

    private long fileSize(String fileName) {
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::fileSize'");
        // tx.concurrencyMgr().readFile(fileName);
        // return VanillaDb.fileMgr().size(fileName);
    }

    private SearchKey getKey() {
        throw new UnsupportedOperationException("Unimplemented method 'IVFFlatIndex::getKey'");
        // Constant[] vals = new Constant[keyType.length()];
        // for (int i = 0; i < vals.length; i++)
        // vals[i] = rf.getVal(keyFieldName(i));
        // return new SearchKey(vals);
    }
}
