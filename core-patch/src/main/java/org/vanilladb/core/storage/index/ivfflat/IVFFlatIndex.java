package org.vanilladb.core.storage.index.ivfflat;

import org.vanilladb.core.sql.*;
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
import org.vanilladb.core.util.CoreProperties;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import static org.vanilladb.core.sql.Type.*;

public class IVFFlatIndex extends Index {
    private static final int CENTROID_COUNT;
    private static final int MAX_CENTROID_COUNT;
    private static final int MIN_VECTOR_COUNT;
    private static final int DIST_DIFF_THRESHOLD;
    private static final int MOVE_DIST_THRESHOLD;
    private static final int MAX_ITERATIONS;
    private static final int REINDEX_THRESHOLD;
    static int insert_count = 0;

    static {
        CENTROID_COUNT = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".CENTROID_COUNT", 800);
        MAX_CENTROID_COUNT = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".MAX_CENTROID_COUNT", 50);
        MIN_VECTOR_COUNT = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".MIN_VECTOR_COUNT", 20);
        DIST_DIFF_THRESHOLD = CoreProperties.getLoader().getPropertyAsInteger(
            IVFFlatIndex.class.getName() + ".DIST_DIFF_THRESHOLD", 75);
        MOVE_DIST_THRESHOLD = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".MOVE_DIST_THRESHOLD", 200);
        MAX_ITERATIONS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".MAX_ITERATIONS", 30);
        REINDEX_THRESHOLD = CoreProperties.getLoader().getPropertyAsInteger(
                IVFFlatIndex.class.getName() + ".REINDEX_THRESHOLD", 40000);
    }

    TableInfo tiCentroid;
    List<VectorConstant> centroids;
    List<List<IVFFlatVectorRecordId>> vectors;
    RecordFile rfCentroid, recordFile;
    List<RecordFile> recordFiles = new LinkedList<>();
    double totalCentroidMoveDist;
    boolean isBeforeFirsted = false;
    
    public IVFFlatIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
        tiCentroid = new TableInfo(ii.indexName() + "_centroid", centroidSchema(keyType));
        rfCentroid = tiCentroid.open(tx, false);
        if (rfCentroid.fileSize() == 0)
            RecordFile.formatFileHeader(tiCentroid.fileName(), tx);
    }

    public static Schema centroidSchema(SearchKeyType keyType) {
        Schema sch = new Schema();
        sch.addField("centroid", keyType.get(0));
        sch.addField("idx", INTEGER);
        sch.addField("item_count", INTEGER);
        return sch;
    }

    private static Schema clusterSchema(SearchKeyType keyType) {
        Schema sch = new Schema();
        sch.addField("key", keyType.get(0));
        sch.addField("block", BIGINT);
        sch.addField("id", INTEGER);
        return sch;
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();

        SearchKey searchKey = searchRange.asSearchKey();
        if (searchKey.length() != 1)
            throw new UnsupportedOperationException();

        VectorConstant vector = (VectorConstant) searchKey.get(0);
        EuclideanFn euclideanFn = new EuclideanFn("dist");

        ArrayList<Integer> closestCentroidIndices = new ArrayList<>();

        PriorityQueue<IVFFlatDist> distPQ = new PriorityQueue<>(comparator);
        
        // calculate and add all distances of all centroids and vector to distPQ
        rfCentroid.beforeFirst();
        while (rfCentroid.next()) {
            VectorConstant centroid = (VectorConstant) rfCentroid.getVal("centroid");
            IntegerConstant idx = (IntegerConstant) rfCentroid.getVal("idx");
            IntegerConstant itemsCount = (IntegerConstant) rfCentroid.getVal("item_count");

            euclideanFn.setQueryVector(centroid);
            distPQ.add(new IVFFlatDist((int) idx.asJavaVal(), euclideanFn.distance(vector), (int) itemsCount.asJavaVal()));
        }
        
        // find closests clusters' centroids
        double lastDist = 0;
        int totalItemCount = 0;

        while (!distPQ.isEmpty()) {
            boolean continueLoop = totalItemCount < MIN_VECTOR_COUNT;
            boolean withinThreshold = 
                (distPQ.peek().getDist() - lastDist) < DIST_DIFF_THRESHOLD &&
                closestCentroidIndices.size() < MAX_CENTROID_COUNT;

            if (continueLoop || withinThreshold) {
                IVFFlatDist dis = distPQ.poll();
                closestCentroidIndices.add(dis.getIdx());
                totalItemCount += dis.getItemCount();
                lastDist = dis.getDist();
            } else {
                break;
            }
        }

        
        // insert dummy centroid if there is no closest centroid
        if (closestCentroidIndices.size() == 0) {
            closestCentroidIndices.add(0);
        }

        // open the record files for the closest centroids
        for (int centroidIdx : closestCentroidIndices) {
            TableInfo tableInfo = new TableInfo(ii.indexName() + centroidIdx, clusterSchema(keyType));
            RecordFile recordFile = tableInfo.open(tx, false);
            if (recordFile.fileSize() == 0) {
                RecordFile.formatFileHeader(tableInfo.fileName(), tx);
            }
            recordFile.beforeFirst();
            recordFiles.add(recordFile);
        }
        recordFile = recordFiles.remove(0);
        isBeforeFirsted = true;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted) {
            throw new IllegalStateException();
        }

        if (recordFile.next()) {
            return true;
        }

        while (!recordFiles.isEmpty()) {
            recordFile = recordFiles.remove(0);
            if (recordFile.next()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public RecordId getDataRecordId() {
        long block = (long) recordFile.getVal("block").asJavaVal();
        BlockId blk = new BlockId(ii.tableName(), block);
        int rid = (int) recordFile.getVal("id").asJavaVal();
        return new RecordId(blk, rid);
    }

    // Insert a vector to the closest cluster
    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        if (insert_count < CENTROID_COUNT) { // Add as centroid
            rfCentroid.insert();
            rfCentroid.setVal("centroid", key.get(0));
            rfCentroid.setVal("idx", new IntegerConstant(insert_count));
            rfCentroid.setVal("item_count", new IntegerConstant(1));
        }
        insert_count++;
        beforeInsert(new SearchRange(key));

        // // Log the logical operation starts
        // if (doLogicalLogging)
        //     tx.recoveryMgr().logLogicalStart();

        // Insert the data
        recordFile.insert();
        recordFile.setVal("key", key.get(0));
        recordFile.setVal("block", new BigIntConstant(dataRecordId.block().number()));
        recordFile.setVal("id", new IntegerConstant(dataRecordId.id()));

        // // Log the logical operation ends
        // if (doLogicalLogging)
        //     tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
        //             dataRecordId.block().number(), dataRecordId.id());

        if (insert_count++ % REINDEX_THRESHOLD == 0) { // Reindex centroids
            reindexCentroids();
        }
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        EuclideanFn euclideanFn = new EuclideanFn("dist");
        euclideanFn.setQueryVector((VectorConstant) key.get(0));
        double minDist = Double.MAX_VALUE;
        int closestCentroidIndex = 0;

        rfCentroid.beforeFirst();
        while (rfCentroid.next()) {
            VectorConstant centroid = (VectorConstant) rfCentroid.getVal("centroid");
            int idx = (int) ((IntegerConstant) rfCentroid.getVal("idx")).asJavaVal();
            double dist = euclideanFn.distance(centroid);
            if (dist < minDist) {
                minDist = dist;
                closestCentroidIndex = idx;
            }
        }

        TableInfo ti = new TableInfo(ii.indexName() + closestCentroidIndex, clusterSchema(keyType));
        recordFile = ti.open(tx, false);
        recordFile.beforeFirst();
        while (recordFile.next()) {
            long block = (long) recordFile.getVal("block").asJavaVal();
            int rid = (int) recordFile.getVal("id").asJavaVal();
            if (block == dataRecordId.block().number() && rid == dataRecordId.id()) {
                recordFile.delete();
                break;
            }
        }

        rfCentroid.beforeFirst();
        while (rfCentroid.next()) {
            IntegerConstant idx = (IntegerConstant) rfCentroid.getVal("idx");
            if ((int) idx.asJavaVal() == closestCentroidIndex) {
                int itemCount = (int) rfCentroid.getVal("item_count").asJavaVal();
                rfCentroid.setVal("item_count", new IntegerConstant(itemCount-1));
                break;
            }
        }
    }

    @Override
    public void close() {
        if (rfCentroid != null)
            rfCentroid.close();
        if (recordFile != null)
            recordFile.close();
    }

    @Override
    public void preLoadToMemory() {
        // Do nothing
    }  
    
    private void beforeInsert(SearchRange searchRange) {
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();
            
        SearchKey searchKey = searchRange.asSearchKey();
        if (searchKey.length() != 1)
            throw new UnsupportedOperationException();

        VectorConstant vector = (VectorConstant) searchKey.get(0);
        EuclideanFn euclideanFn = new EuclideanFn("dist");

        double minDist = Double.MAX_VALUE;
        int closestCentroidIndex = 0;

        rfCentroid.beforeFirst();
        while (rfCentroid.next()) {
            VectorConstant centroid = (VectorConstant) rfCentroid.getVal("centroid");
            int idx = (int) ((IntegerConstant) rfCentroid.getVal("idx")).asJavaVal();

            euclideanFn.setQueryVector(centroid);
            double dist = euclideanFn.distance(vector);
            
            if (dist < minDist) {
                minDist = dist;
                closestCentroidIndex = idx;
            }
        }

        TableInfo ti = new TableInfo(ii.indexName() + closestCentroidIndex, clusterSchema(keyType));
        recordFile = ti.open(tx, false);
        if (recordFile.fileSize() == 0) {
            RecordFile.formatFileHeader(ti.fileName(), tx);
        }
        recordFile.beforeFirst();
    }

    // K-means algorithm reindexing
    private void reindexCentroids() {
        deleteVectorsRF();

        int iteration = 0;
        do {
            updateCentroids();
            assignVectors();
            iteration++;
        } while (iteration < MAX_ITERATIONS && totalCentroidMoveDist > MOVE_DIST_THRESHOLD);

        writeVectorsRF();
    }

    // Save all vectors and centroids, and delete all record files of vectors and centroids
    private void deleteVectorsRF() {
        vectors = new ArrayList<>(CENTROID_COUNT);
        for(int i = 0; i < CENTROID_COUNT; i++) {
            vectors.add(new ArrayList<>());
        }
        centroids = new ArrayList<>();
        rfCentroid.beforeFirst();
        int centroid_idx;
        while (rfCentroid.next()) {
            VectorConstant centroid = (VectorConstant) rfCentroid.getVal("centroid");
            centroids.add(centroid);
            IntegerConstant idx = (IntegerConstant) rfCentroid.getVal("idx");
            centroid_idx = (int) idx.asJavaVal();

            TableInfo ti = new TableInfo(ii.indexName() + centroid_idx, clusterSchema(keyType));
            recordFile = ti.open(tx, false);
            recordFile.beforeFirst();
            while (recordFile.next()) {
                vectors.get(centroid_idx).add(new IVFFlatVectorRecordId(
                        (VectorConstant) recordFile.getVal("key"),
                        (BigIntConstant) recordFile.getVal("block"),
                        (IntegerConstant) recordFile.getVal("id")
                ));
                recordFile.delete();
            }

            recordFile.close();
            rfCentroid.delete();
        }
        rfCentroid.close();
        recordFile = null;
        rfCentroid = null;
    }

    // Recalculate new centroids by computing means of vectors in each cluster
    private void updateCentroids() {
        totalCentroidMoveDist = 0;
        EuclideanFn euclideanFn = new EuclideanFn("dist");

        for (int i = 0; i < CENTROID_COUNT; i++) {
            double[] centroid_calc;
            float[] centroid = centroids.get(i).asJavaVal().clone();

            if (!vectors.get(i).isEmpty()) {
                float[] v;
                centroid_calc = new double[centroid.length];

                for (int j = 0; j < vectors.get(i).size(); j++) {
                    v = vectors.get(i).get(j).vec.asJavaVal();
                    for (int k = 0; k < v.length; k++) {
                        centroid_calc[k] += (double) v[k] / vectors.get(i).size();
                    }
                }

                for (int j = 0; j < centroid.length; j++) {
                    centroid[j] = (float) centroid_calc[j];
                }
            }
            euclideanFn.setQueryVector(new VectorConstant(centroid));
            totalCentroidMoveDist = euclideanFn.distance(centroids.get(i));
            centroids.set(i, new VectorConstant(centroid));
        }
    }

    // Assign each vectors to nearest centroid
    private void assignVectors() {
        List<List<IVFFlatVectorRecordId>> newClusteredVectors = new ArrayList<>(CENTROID_COUNT);
        for(int i = 0; i < CENTROID_COUNT; i++) {
            newClusteredVectors.add(new ArrayList<>());
        }
        EuclideanFn euclideanFn = new EuclideanFn("dist");
        for(int i = 0; i < CENTROID_COUNT; i++) {
            for (int j = 0; j < vectors.get(i).size(); j++) {
                IVFFlatVectorRecordId vr = vectors.get(i).get(j);
                euclideanFn.setQueryVector(vr.vec);
                double minDist = Double.MAX_VALUE, currDist;
                int idx = 0;

                for (int k = 0; k < CENTROID_COUNT; k++) {
                    currDist = euclideanFn.distance(centroids.get(k));
                    if (currDist < minDist) {
                        minDist = currDist;
                        idx = k;
                    }
                }
                newClusteredVectors.get(idx).add(vr);
            }
        }
        vectors = newClusteredVectors;
    }

    private void writeVectorsRF() {
        rfCentroid = tiCentroid.open(tx, false);
        if (rfCentroid.fileSize() == 0)
            RecordFile.formatFileHeader(tiCentroid.fileName(), tx);

        for(int i = 0; i < CENTROID_COUNT; i++) {
            var vectorCluster = vectors.get(i);

            rfCentroid.insert();
            rfCentroid.setVal("centroid", centroids.get(i));
            rfCentroid.setVal("idx", new IntegerConstant(i));
            rfCentroid.setVal("item_count", new IntegerConstant(vectorCluster.size()));

            TableInfo ti = new TableInfo(ii.indexName() + i, clusterSchema(keyType));
            recordFile = ti.open(tx, false);
            if (recordFile.fileSize() == 0)
                RecordFile.formatFileHeader(ti.fileName(), tx);

            for (int j = 0; j < vectorCluster.size(); j++) {
                IVFFlatVectorRecordId vr = vectorCluster.get(j);
                recordFile.insert();
                recordFile.setVal("key", vr.vec);
                recordFile.setVal("block", vr.blk);
                recordFile.setVal("id", vr.rid);
            }
            recordFile.close();
        }
        // rfCentroid.close(); // Close rfCentroid or not?
    }

    Comparator<IVFFlatDist> comparator = (a, b) -> {
        double aDist = a.getDist();
        double bDist = b.getDist();
        if (aDist - bDist < 0)
            return -1;
        else if (aDist - bDist > 0)
            return 1;
        return 0;
    };
}