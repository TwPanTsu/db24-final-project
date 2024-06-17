package org.vanilladb.core.storage.index.ivfflat;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;

public class IvfflatIndexTrainer {
    private static Logger logger = Logger.getLogger(IvfflatIndexTrainer.class.getName());
    private IvfflatIndex ivfflatIndex;
    private SearchKeyType keyType;
    private Transaction tx;
    private IndexInfo ii;
    private RecordFile recordFile;
    private VectorConstant[] vectorIndex;
    private int NUM_CLUSTERSS;
    private Constant MAX_VECTOR_CONSTANT;

    private long startTime = System.currentTimeMillis();
    private final long timeLimit = 60000 * 15 ; // Training time limit
    private final double sampleRate = 0.5;
    private final int clusterMin = 20;

    public IvfflatIndexTrainer(IvfflatIndex ivfflatIndex, SearchKeyType keyType, Transaction tx, IndexInfo ii, RecordFile recordFile, VectorConstant[] vectorIndex, int NUM_CLUSTERSS, Constant MAX_VECTOR_CONSTANT) {
        this.ivfflatIndex = ivfflatIndex;
        this.keyType = keyType;
        this.tx = tx;
        this.ii = ii;
        this.recordFile = recordFile;
        this.vectorIndex = vectorIndex;
        this.NUM_CLUSTERSS = NUM_CLUSTERSS;
        this.MAX_VECTOR_CONSTANT = MAX_VECTOR_CONSTANT;
    }

    /**
     * Clears existing data in all buckets.
     */
    private void clearBuckets() {
        for (int index = 0; index < NUM_CLUSTERSS; index++) {
            String bucketTableName = ii.indexName() + "-" + index;
            TableInfo bucketTableInfo = VanillaDb.catalogMgr().getTableInfo(bucketTableName, tx);
            if (bucketTableInfo != null) {
                RecordFile bucketRecordFile = bucketTableInfo.open(tx, false);
                bucketRecordFile.beforeFirst();
                while (bucketRecordFile.next()) {
                    bucketRecordFile.delete();
                }
                logger.info(bucketTableName + ":drop");
                bucketRecordFile.close();
            }
        }
    }

    /**
     * Samples records from the main table and distributes them to buckets.
     * 
     * @param tableInfo the table information
     * @param fieldName the field name to sample
     */
    private void sampleAndDistributeRecords(TableInfo tableInfo, String fieldName) {
        recordFile.beforeFirst();
        RecordFile trainingRecordFile = tableInfo.open(tx, false);
        trainingRecordFile.beforeFirst();
        int indexNum = 0;

        while (trainingRecordFile.next()) {
            if (Math.random() > this.sampleRate) {
                continue;
            }
            String bucketTableName = ii.indexName() + "-" + indexNum;
            TableInfo bucketTableInfo = VanillaDb.catalogMgr().getTableInfo(bucketTableName, tx);
            if (bucketTableInfo == null) {
                VanillaDb.catalogMgr().createTable(bucketTableName, ivfflatIndex.schema(keyType), tx);
                bucketTableInfo = VanillaDb.catalogMgr().getTableInfo(bucketTableName, tx);
            }
            RecordFile bucketRecordFile = bucketTableInfo.open(tx, false);
            bucketRecordFile.insert();
            bucketRecordFile.setVal(IvfflatIndex.SCHEMA_KEY, trainingRecordFile.getVal(fieldName));
            bucketRecordFile.setVal(IvfflatIndex.SCHEMA_RID_BLOCK, new BigIntConstant(trainingRecordFile.currentRecordId().block().number()));
            bucketRecordFile.setVal(IvfflatIndex.SCHEMA_RID_ID, new IntegerConstant(trainingRecordFile.currentRecordId().id()));
            bucketRecordFile.close();
            indexNum = (indexNum + 1) % NUM_CLUSTERSS;
        }

        trainingRecordFile.close();
    }

    /**
     * Calculates the number of training records.
     * 
     * @return the number of training records
     */
    private int TrainCount() {
        recordFile.beforeFirst();
        int trainingRecordCount = 0;
        while (recordFile.next()) {
            trainingRecordCount++;
        }
        return trainingRecordCount;
    }

    /**
     * Performs k-means clustering.
     * 
     * @param meanCount the mean count for k-means
     */
    private void KMeansClustering(int meanCount) {
        long epochTime = 0;
        long insertTime = 0;

        for (int i = 0; true; i++) {
            long trainingStartTime = System.currentTimeMillis();
            logger.info("Training epoch " + i);
            boolean breakKMeans = true;
            LinkedList<Constant> outMeans = new LinkedList<>();
            int outCount = -1;
            Constant outSum = null;

            for (int index = 0; index < NUM_CLUSTERSS; index++) {
                Constant sum = null;
                int count = -1;
                Constant mean = vectorIndex[index];
                String bucketTableName = ii.indexName() + "-" + index;
                TableInfo bucketTableInfo = VanillaDb.catalogMgr().getTableInfo(bucketTableName, tx);
                if (bucketTableInfo == null) {
                    continue;
                } else {
                    RecordFile bucketRecordFile = bucketTableInfo.open(tx, false);
                    bucketRecordFile.beforeFirst();
                    while (bucketRecordFile.next()) {
                        if (count == -1) {
                            sum = bucketRecordFile.getVal(IvfflatIndex.SCHEMA_KEY);
                            count = 1;
                        } else {
                            sum = sum.add(bucketRecordFile.getVal(IvfflatIndex.SCHEMA_KEY));
                            count++;
                        }
                        if (count > meanCount) {
                            if (outCount == -1) {
                                outSum = bucketRecordFile.getVal(IvfflatIndex.SCHEMA_KEY);
                                outCount = 1;
                            } else {
                                outSum = outSum.add(bucketRecordFile.getVal(IvfflatIndex.SCHEMA_KEY));
                                outCount++;
                            }
                            if (outCount >= meanCount) {
                                outMeans.add(outSum.div(new IntegerConstant(outCount)));
                                outCount = -1;
                            }
                        }
                        bucketRecordFile.delete();
                    }
                    if (sum != null) {
                        mean = sum.div(new IntegerConstant(count));
                        if (!mean.equals(vectorIndex[index])) {
                            breakKMeans = false;
                            vectorIndex[index] = (VectorConstant) mean;
                        }
                    }
                    if ((double)count < (double)this.clusterMin * this.sampleRate) {
                        breakKMeans = false;
                        vectorIndex[index] = (VectorConstant) MAX_VECTOR_CONSTANT;
                    }
                    bucketRecordFile.close();
                }
            }

            balanceMeans(outMeans);

            if ((System.currentTimeMillis() - this.startTime) > this.timeLimit - epochTime - insertTime / this.sampleRate) {
                insertFinalMeansToIndex();
                break;
            }
            else {
                insertSampledRecordsToIndex(meanCount);
            }

            insertTime = System.currentTimeMillis();
            insertTime = System.currentTimeMillis() - insertTime;
            epochTime = System.currentTimeMillis() - trainingStartTime;
        }
    }

    /**
     * Balances the means for k-means clustering.
     * 
     * @param outMeans the list of means to balance
     */
    private void balanceMeans(LinkedList<Constant> outMeans) {
        for (int index = 0; index < NUM_CLUSTERSS; index++) {
            if (MAX_VECTOR_CONSTANT.equals(vectorIndex[index]) && !outMeans.isEmpty()) {
                vectorIndex[index] = (VectorConstant) outMeans.pop();
            }
        }
    }

    /**
     * Inserts the final means into the index.
     */
    private void insertFinalMeansToIndex() {
        Map<String, Constant> fieldValues = new HashMap<>();
        recordFile.beforeFirst();
        while (recordFile.next()) {
            fieldValues.put(IvfflatIndex.SCHEMA_KEY, recordFile.getVal(IvfflatIndex.SCHEMA_KEY));
            ivfflatIndex.insert(new SearchKey(ii.fieldNames(), fieldValues), recordFile.currentRecordId(), true);
        }
    }

    /**
     * Inserts sampled records into the index.
     * 
     * @param meanCount the mean count for k-means
     */
    private void insertSampledRecordsToIndex(int meanCount) {
        Map<String, Constant> fieldValues = new HashMap<>();
        recordFile.beforeFirst();
        while (recordFile.next()) {
            if (Math.random() > this.sampleRate) {
                continue;
            }
            fieldValues.put(IvfflatIndex.SCHEMA_KEY, recordFile.getVal(IvfflatIndex.SCHEMA_KEY));
            ivfflatIndex.insert(new SearchKey(ii.fieldNames(), fieldValues), recordFile.currentRecordId(), true);
        }
    }

    /**
     * Updates bucket statistics and finalizes the index.
     */
    private void updateBucketStatisticsAndFinalizeIndex() {
        recordFile.beforeFirst();
        for (int index = 0; index < NUM_CLUSTERSS; index++) {
            recordFile.next();
            recordFile.setVal(IvfflatIndex.SCHEMA_KEY, vectorIndex[index]);
            recordFile.setVal(IvfflatIndex.SCHEMA_ID, new IntegerConstant(index));
            String bucketTableName = ii.indexName() + "-" + index;
            TableInfo bucketTableInfo = VanillaDb.catalogMgr().getTableInfo(bucketTableName, tx);
            if (bucketTableInfo == null) {
                recordFile.delete();
                continue;
            } else {
                RecordFile bucketRecordFile = bucketTableInfo.open(tx, false);
                int count = 0;
                bucketRecordFile.beforeFirst();
                while (bucketRecordFile.next()) {
                    count++;
                }
                logger.info(bucketTableName + ":count:" + count);
                bucketRecordFile.close();
                if (count < this.clusterMin) {
                    recordFile.delete();
                }
            }
        }
        recordFile.close();
    }

    /**
     * Trains the IVFFlat index.
     */
    public void trainIndex() {
        this.startTime = System.currentTimeMillis();

        // Initialize the index if not already present
        String tableName = ii.indexName();
        String fieldName = ii.fieldNames().get(0);
        TableInfo tableInfo = VanillaDb.catalogMgr().getTableInfo(tableName, tx);
        if (tableInfo == null) {
            VanillaDb.catalogMgr().createTable(tableName, IvfflatIndex.indexSchema(keyType), tx);
            tableInfo = VanillaDb.catalogMgr().getTableInfo(tableName, tx);
            this.recordFile = tableInfo.open(tx, false);
            recordFile.beforeFirst();
            logger.info("IVFFlat initializing...");
            ivfflatIndex.initializeIndex();
        }

        // Clear existing data in all buckets
        clearBuckets();

        // Sample records from the main table and distribute them to buckets
        sampleAndDistributeRecords(tableInfo, fieldName);

        // Calculate initial mean count for k-means clustering
        int meanCount = (TrainCount() + NUM_CLUSTERSS - 1) / NUM_CLUSTERSS;
        
        // Perform k-means clustering
        KMeansClustering(meanCount);

        // Update bucket statistics and finalize the index
        updateBucketStatisticsAndFinalizeIndex();

        logger.info("Training completed.");
    }
}
