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

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.lang.Math;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
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
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.server.VanillaDb;

// import org.vanilladb.core.storage.index.cluster.VectorHeap;
import java.util.concurrent.locks.ReadWriteLock;

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
    private static final String SCHEMA_CENTROID_ID = "cid", SCHEMA_KEY = "key";
    private static final String SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id"; // block and id to the original table

    public static final int NUM_CLUSTERS_MAX;
    public static final int NUM_CLUSTERS_TOTAL;
    private ConcurrencyMgr ccMgr;
    private IndexInfo ii;
    private TableInfo ti;
    private int clusterNum;
    // private static int insertNum = 0; // Times of insertion
    // private static int numVectors = 0;
    // private static int refactorNum = 1000;
    // private static int addNum = 100;
    // private static int initClusters = 5;
    // private static int maxConvergeTimes = 5;
    // private static ReadWriteLock rwLock;
    // private int numUnpackClusters;

    static {
        NUM_CLUSTERS_MAX = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_CLUSTERS_MAX", 10);
        NUM_CLUSTERS_TOTAL = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".NUM_CLUSTERS_TOTAL", 2000);
    }

    public IVFFlatIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
        this.ccMgr = tx.concurrencyMgr();
        this.ii = ii;

        this.ti = getIndexUsedTableInfo(ii.indexName(), schema(keyType));
        this.rf = ti.open(tx, false);

        // Count the number of clusters
        if (rf.fileSize() == 0) {
            RecordFile.formatFileHeader(ti.fileName(), tx);
            this.clusterNum = 0;
        } else {
            rf.beforeFirst();
            while (rf.next()) {
                this.clusterNum++;
            }
        }
        rf.close();

    }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
        return (totRecs / rpb) / NUM_CLUSTERS_MAX;
    }

    private static String keyFieldName(int index) {
        return SCHEMA_KEY + index;
    }

    private TableInfo getIndexUsedTableInfo(String tableName, Schema schema) {
        TableInfo ti = VanillaDb.catalogMgr().getTableInfo(tableName, tx);
        if (ti == null) {
            Schema sch = schema;
            VanillaDb.catalogMgr().createTable(tableName, sch, tx);
            ti = VanillaDb.catalogMgr().getTableInfo(tableName, tx);
        }
        return ti;
    }

    /**
     * Returns the schema of the index records.
     * 
     * @param fldType
     *                the type of the indexed field
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
    private RecordFile rf;
    private boolean isBeforeFirsted;

    @Override
    public void preLoadToMemory() {
        for (int i = 0; i < NUM_CLUSTERS_MAX; i++) {
            String tblname = ii.indexName() + i + ".centroid";
            long size = fileSize(tblname);
            BlockId blk;
            for (int j = 0; j < size; j++) {
                blk = new BlockId(tblname, j);
                tx.bufferMgr().pin(blk);
            }
        }
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
        // TODO
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();
        System.out.println("SearchRange: " + searchRange.asSearchKey().toString());
        this.searchKey = searchRange.asSearchKey();
        // for (int i = 0; i < this.clusterNum; i++) {

        // }

        isBeforeFirsted = true;
    }

    /**
     * Moves to the next index record having the search key.
     * 
     * @see Index#next()
     */
    @Override
    public boolean next() {
        // TODO
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
        // beforeFirst(new SearchRange(key));

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
                return;
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
}
