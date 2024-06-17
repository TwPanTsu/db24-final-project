package org.vanilladb.core.storage.index.ivf;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.ByteVectorConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.VectorType;
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
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.ByteHelper;
import org.vanilladb.core.util.CoreProperties;

public class IVFSq8DirectIndex extends Index {
    private static Logger logger = Logger.getLogger(IVFSq8DirectIndex.class.getName());

    /**
     * A field name of the schema of index records.
     */
    private static final String SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id";

    public final static int NUM_CENTROIDS;
    public final static int NUM_PROBE_BUCKETS;
    // public final static int PROBE_THREADS = Runtime.getRuntime().availableProcessors();
    public final static int PROBE_THREADS = 1;

    static {
        NUM_CENTROIDS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFSq8DirectIndex.class.getName() + ".NUM_CENTROIDS", 16384);
        NUM_PROBE_BUCKETS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFSq8DirectIndex.class.getName() + ".NUM_PROBE_BUCKETS", 2);
    }

    public static int numCentroidBlocks(SearchKeyType keyType) {
        // TODO: I thought Buffer.BUFFER_SIZE equals Page.BLOCK_SIZE.
        int fpb = Buffer.BUFFER_SIZE / ByteHelper.FLOAT_SIZE;
        return (IVFSq8DirectIndex.NUM_CENTROIDS * keyType.get(0).getArgument() + fpb - 1) / fpb;
    }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        // Assume that the vector field accounts for most of the space
        int vpb = Buffer.BUFFER_SIZE / keyType.get(0).maxSize();
        return numCentroidBlocks(keyType) + (totRecs / vpb) / NUM_CENTROIDS;
    }

    private class ProbeTask extends Task {
        private EuclideanFn distFn;
        private int low, high;  // The assigned task
        private CountDownLatch doneSignal;
        // Accessible from the outside
        double[] dists;
        int[] probedClusters;
        int probeId, probeCount;

        ProbeTask(int n, EuclideanFn distFn, int probeCount, CountDownLatch doneSignal) {
            this.distFn = distFn;
            this.probeCount = probeCount;
            this.doneSignal = doneSignal;
            low = n * IVFSq8DirectIndex.NUM_CENTROIDS / PROBE_THREADS;
            high = (n + 1) * IVFSq8DirectIndex.NUM_CENTROIDS / PROBE_THREADS;
            dists = new double[probeCount];
            probedClusters = new int[probeCount];
            probeId = 0;
        }

        @Override
        public void run() {
            for (int i = low; i < high; ++i) {
                VectorConstant centroid = getCentroidVector(i);
                double dist = distFn.distance(centroid);
                int j = probeId;
                if (probeId < probeCount) ++probeId;
                while (j > 0 && dist < dists[j - 1]) {
                    if (j < probeId) {
                        dists[j] = dists[j - 1];
                        probedClusters[j] = probedClusters[j - 1];
                    }
                    --j;
                }
                if (j < probeId) {
                    dists[j] = dist;
                    probedClusters[j] = i;
                }
            }
            probeCount = probeId; probeId = 0;
            doneSignal.countDown();
        }
    }

    public String centroidName() {
        return ii.indexName() + "_centroids.idx";
    }

    public VectorConstant getCentroidVector(int bucket) {
        int fpb = Buffer.BUFFER_SIZE / ByteHelper.FLOAT_SIZE;
        VectorType T = (VectorType)keyType.get(0);
        int dimension = T.getArgument();
        int id0 = bucket * dimension, id1 = (bucket + 1) * dimension;
        int blkNum0 = id0 / fpb, blkNum1 = (id1 - 1) / fpb;
        if (blkNum0 == blkNum1) {
            int offset = id0 % fpb * ByteHelper.FLOAT_SIZE;
            Buffer buff = tx.bufferMgr().pin(new BlockId(centroidName(), blkNum0));
            VectorConstant v = (VectorConstant)buff.getVal(offset, T);
            tx.bufferMgr().unpin(buff);
            return v;
        } else {
            int sz0 = fpb - id0 % fpb, sz1 = dimension - sz0;
            Buffer buff = tx.bufferMgr().pin(new BlockId(centroidName(), blkNum0));
            float[] a = new float[dimension];
            VectorConstant tmp = (VectorConstant)buff.getVal(
                Buffer.BUFFER_SIZE - Type.VECTOR(sz0).maxSize(), Type.VECTOR(sz0));
            tx.bufferMgr().unpin(buff);
            System.arraycopy(tmp.asJavaVal(), 0, a, 0, sz0);

            buff = tx.bufferMgr().pin(new BlockId(centroidName(), blkNum1));
            tmp = (VectorConstant)buff.getVal(0, Type.VECTOR(sz1));
            tx.bufferMgr().unpin(buff);
            System.arraycopy(tmp.asJavaVal(), 0, a, sz0, sz1);
            return new VectorConstant(a);
        }
    }

    public void setCentroidVector(int bucket, VectorConstant centroid) {
        int fpb = Buffer.BUFFER_SIZE / ByteHelper.FLOAT_SIZE;
        int dimension = ((VectorType)keyType.get(0)).getArgument();
        int id0 = bucket * dimension, id1 = (bucket + 1) * dimension;
        int blkNum0 = id0 / fpb, blkNum1 = (id1 - 1) / fpb;
        if (blkNum0 == blkNum1) {
            int offset = id0 % fpb * ByteHelper.FLOAT_SIZE;
            Buffer buff = tx.bufferMgr().pin(new BlockId(centroidName(), blkNum0));
            buff.setVal(offset, centroid, tx.getTransactionNumber(), null);
            tx.bufferMgr().unpin(buff);
        } else {
            int sz0 = fpb - id0 % fpb;
            VectorConstant tmp = new VectorConstant(Arrays.copyOfRange(centroid.asJavaVal(), 0, sz0));
            Buffer buff = tx.bufferMgr().pin(new BlockId(centroidName(), blkNum0));
            buff.setVal(Buffer.BUFFER_SIZE - tmp.size(), tmp, tx.getTransactionNumber(), null);
            tx.bufferMgr().unpin(buff);

            tmp = new VectorConstant(Arrays.copyOfRange(centroid.asJavaVal(), sz0, dimension));
            buff = tx.bufferMgr().pin(new BlockId(centroidName(), blkNum1));
            buff.setVal(0, tmp, tx.getTransactionNumber(), null);
            tx.bufferMgr().unpin(buff);
        }
    }

    private void probe(VectorConstant query, int probeCount) {
        if (VanillaDb.fileMgr().isFileEmpty(centroidName())) {
            probeId = -1;
            return;
        }
        if (probeCount > IVFSq8DirectIndex.NUM_CENTROIDS)
            throw new RuntimeException();
        EuclideanFn distFn = new EuclideanFn(ii.fieldNames().get(0));
        distFn.setQueryVector(query);

        ProbeTask[] tasks = new ProbeTask[PROBE_THREADS];
        CountDownLatch doneSignal = new CountDownLatch(PROBE_THREADS);
        for (int i = 0; i < PROBE_THREADS; ++i) {
            tasks[i] = new ProbeTask(i, distFn, probeCount, doneSignal);
            VanillaDb.taskMgr().runTask(tasks[i]);
        }
        try {
            doneSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        double[] dists = new double[probeCount];
        probedClusters = new int[probeCount];
        for (probeId = 0; probeId < probeCount; ++probeId) {
            ProbeTask currentBest = null;
            int i;
            for (i = 0; i < PROBE_THREADS; ++i) {
                if (tasks[i].probeId == tasks[i].probeCount) continue;
                if (currentBest == null
                        || tasks[i].dists[tasks[i].probeId] < currentBest.dists[currentBest.probeId])
                    currentBest = tasks[i];
            }
            double dist = currentBest.dists[currentBest.probeId];

            for (i = probeId; i > 0 && dist < dists[i - 1]; --i) {
                dists[i] = dists[i - 1];
                probedClusters[i] = probedClusters[i - 1];
            }
            dists[i] = dist;
            probedClusters[i] = currentBest.probedClusters[currentBest.probeId++];
        }
        probeId = 0;
    }

	private SearchKey searchKey;
    private Schema tableSchema, sq8DirectSchema;
    private RecordFile rf;
    private int[] probedClusters;
    private int probeId;

    public IVFSq8DirectIndex(IndexInfo ii, SearchKeyType keyType, Schema schema, Transaction tx) {
        super(ii, keyType, tx);
        tableSchema = schema;
        sq8DirectSchema = new Schema();
        for (String fldname : tableSchema.fields())
            if (tableSchema.type(fldname) instanceof VectorType)
                sq8DirectSchema.addField(fldname, Type.BYTEVECTOR(tableSchema.type(fldname).getArgument()));
            else
                sq8DirectSchema.addField(fldname, tableSchema.type(fldname));
        sq8DirectSchema.addField(SCHEMA_RID_BLOCK, BIGINT);
        sq8DirectSchema.addField(SCHEMA_RID_ID, INTEGER);
        probeId = -1;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        close();

        searchKey = searchRange.asSearchKey();
        probe((VectorConstant)searchKey.get(0), NUM_PROBE_BUCKETS);
        String tblname = ii.indexName() + probedClusters[0];
        TableInfo ti = new TableInfo(tblname, sq8DirectSchema);

        // the underlying record file should not perform logging
        rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();
    }

    @Override
    public boolean next() {
        if (probeId == -1)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '"
                    + ii.indexName() + "'");
        
        while (!rf.next()) {
            rf.close();
            if (++probeId == probedClusters.length) {
                rf = null;
                return false;
            }

            String tblname = ii.indexName() + probedClusters[probeId];
            TableInfo ti = new TableInfo(tblname, sq8DirectSchema);

            // the underlying record file should not perform logging
            rf = ti.open(tx, false);
    
            // initialize the file header if needed
            if (rf.fileSize() == 0)
                RecordFile.formatFileHeader(ti.fileName(), tx);
            rf.beforeFirst();
        }
        return true;
    }

    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        int i;
        close();

        // "key" has all the fields, not just the key field
        searchKey = key;
        for (i = 0; !(searchKey.get(i) instanceof VectorConstant); ++i) {}
        probe((VectorConstant)searchKey.get(i), 1);
        if (probeId == -1) return;  // Still loading testbed

        String tblname = ii.indexName() + probedClusters[0];
        TableInfo ti = new TableInfo(tblname, sq8DirectSchema);

        // the underlying record file should not perform logging
        rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        // insert the data
        i = 0;
        rf.insert();
        for (String fldname : tableSchema.fields()) {
            Constant c = key.get(i++);
            if (c instanceof VectorConstant)
                rf.setVal(fldname, new ByteVectorConstant((VectorConstant)c));
            else
                rf.setVal(fldname, c);
        }
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
                .number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        close();

        searchKey = key;
        probe((VectorConstant)searchKey.get(0), 1);
        if (probeId == -1) return;  // Probably won't happen

        String tblname = ii.indexName() + probedClusters[0];
        TableInfo ti = new TableInfo(tblname, sq8DirectSchema);
        if (VanillaDb.fileMgr().isFileEmpty(ti.fileName())) return;

        // the underlying record file should not perform logging
        rf = ti.open(tx, false);
        rf.beforeFirst();

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();
        
        // delete the specified entry
        while (next())
            if (getDataRecordId().equals(dataRecordId)) {
                rf.delete();
                break;
            }
        
        if (probeId == probedClusters.length && logger.isLoggable(Level.WARNING))
            logger.warning("IVF_SQ8_DIRECT delete failed to delete a record");

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void close() {
        if (rf != null) {
            rf.close();
            rf = null;
        }
        probedClusters = null;
        probeId = -1;
    }

    @Override
    public void preLoadToMemory() {
        // TODO: this needs NUM_CENTROIDS / 7 buffers, is it enough?
        int size = numCentroidBlocks(keyType);
        BlockId blk;
        for (int j = 0; j < size; j++) {
            blk = new BlockId(centroidName(), j);
            tx.bufferMgr().pin(blk);
        }
    }

    public Constant getVal(String fldName) {
        Constant c = rf.getVal(fldName);
        if (c instanceof ByteVectorConstant)
            return new VectorConstant((ByteVectorConstant)c);
        return c;
    }

}
