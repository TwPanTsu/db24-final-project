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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.materialize.MinHeapPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorType;
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

public class IVFIndex extends Index {

    /**
     * A field name of the schema of index records.
     */
    private static final String SCHEMA_RID_BLOCK = "block",
            SCHEMA_RID_ID = "id";

    private static final int NUM_CLUSTERS = 200; // 預設的簇數量

    private SearchKey searchKey;
    private RecordFile rf;
    private boolean isBeforeFirsted;

    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
    }

    private static Schema centroid_schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        sch.addField("centroid", keyType.get(0));
        sch.addField("cluster_id", INTEGER);
        return sch;
    }

    private static Schema index_schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }

    // private static String keyFieldName(int index) {
    // return SCHEMA_KEY + index;
    // }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(index_schema(keyType));
        return (totRecs / rpb) / NUM_CLUSTERS;
    }

    @Override
    public void preLoadToMemory() {
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            String tblname = ii.indexName() + i + ".tbl";
            long size = fileSize(tblname);
            BlockId blk;
            for (int j = 0; j < size; j++) {
                blk = new BlockId(tblname, j);
                tx.bufferMgr().pin(blk);
            }
        }
        String tblname = ii.indexName() + "_centroid" + ".tbl";
        long size = fileSize(tblname);
        BlockId blk;
        for (int j = 0; j < size; j++) {
            blk = new BlockId(tblname, j);
            tx.bufferMgr().pin(blk);
        }
    }

    @Override
    public void beforeFirst(SearchRange range) {
        throw new UnsupportedOperationException("Use beforeFirst(DistanceFn target) instead.");
    }

    public void beforeFirst(DistanceFn target) {
        close();

        int cluster = findNearestCluster(target);
        String tblname = ii.indexName() + cluster;
        TableInfo ti = new TableInfo(tblname, index_schema(keyType));
        this.rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();

        isBeforeFirsted = true;
    }

    private int findNearestCluster(DistanceFn target) {
        double mindist = Double.MAX_VALUE;
        int mincluster = 0;
        String tblname = ii.indexName() + "_centroid";
        TableInfo ti = new TableInfo(tblname, centroid_schema(keyType));

        if (ti == null) {
            throw new RuntimeException("TableInfo is null for table: " + tblname);
        }

        this.rf = ti.open(tx, false);
        if (rf.fileSize() == 0) {
            RecordFile.formatFileHeader(ti.fileName(), tx);
            train_index();
        }
        rf.beforeFirst();
        while (rf.next()) {
            VectorConstant centroid = (VectorConstant) rf.getVal("centroid");
            double dist = target.distance(centroid);
            if (dist < mindist) {
                mindist = dist;
                mincluster = (Integer) rf.getVal("cluster_id").asJavaVal();
            }
        }
        return mincluster;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '"
                    + ii.indexName() + "'");

        if (rf.next())
            return true;
        else
            return false;
    }

    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        DistanceFn target = new EuclideanFn("temp");
        target.setQueryVector((VectorConstant) key.get(0));
        beforeFirst(target);
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();
        rf.setLogEnabled(false);
        rf.insert();
        rf.setLogEnabled(true);
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block().number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        DistanceFn target = new EuclideanFn("temp");
        target.setQueryVector((VectorConstant) key.get(0));
        beforeFirst(target);

        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        while (next())
            if (getDataRecordId().equals(dataRecordId)) {
                rf.delete();
                return;
            }

        if (doLogicalLogging)
            tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void close() {
        if (rf != null)
            rf.close();
    }

    private long fileSize(String fileName) {
        tx.concurrencyMgr().readFile(fileName);
        return VanillaDb.fileMgr().size(fileName);
    }

    // private SearchKey getKey() {
    // Constant[] vals = new Constant[keyType.length()];
    // for (int i = 0; i < vals.length; i++)
    // vals[i] = rf.getVal(keyFieldName(i));
    // return new SearchKey(vals);
    // }

    private void train_index() {
        // for(int i = 0; i<NUM_CLUSTERS; i++){

        // VectorConstant centroid ;
        // Random random = new Random();
        // float[] vec = new float[128];
        // for (int j = 0; j < 128; j++) {
        // vec[j] = random.nextFloat() * 130;
        // }
        // centroid = new VectorConstant(vec);
        // rf.insert();
        // rf.setVal("centroid", centroid);
        // rf.setVal("cluster_id", new IntegerConstant(i));
        // }

        /////////////////////////////
        /////////////////////////////
        // System.out.println("start training");
        // Plan p = new TablePlan("sift", tx);
        // TableScan s = (TableScan) p.open();

        // List<String> embFields = new ArrayList<String>(1);
        // embFields.add("i_emb");

        // System.out.println("start scan stft");

        // List<float[]> vectors = new ArrayList<>();

        // s.beforeFirst();
        // while (s.next()) {
        // VectorConstant vConst = (VectorConstant) s.getVal(embFields.get(0));
        // vectors.add(vConst.asJavaVal());
        // }
        // s.close();
        /////////////////////////////
        /////////////////////////////
        List<float[]> vectors = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("sift.txt"))) {
            String vectorString;
            while ((vectorString = br.readLine()) != null) {
                vectors.add(new VectorConstant(vectorString).asJavaVal());
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("error read sift.txt");
        }

        //System.out.println("end scan sift");

        SimpleKMeans kmeans = new SimpleKMeans(NUM_CLUSTERS, 20);
        List<float[]> clusterCenters = kmeans.train(vectors);

        for (int i = 0; i < NUM_CLUSTERS; i++) {
            VectorConstant centroid = new VectorConstant(clusterCenters.get(i));
            rf.insert();
            rf.setVal("centroid", centroid);
            rf.setVal("cluster_id", new IntegerConstant(i));
        }
        //System.out.println("end training");
    }
}
