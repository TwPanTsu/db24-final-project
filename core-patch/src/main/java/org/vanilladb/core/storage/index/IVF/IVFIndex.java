package org.vanilladb.core.storage.index.IVF;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
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

public class IVFIndex extends Index {

    /**
     * A field name of the schema of index records.
     */
    private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
            SCHEMA_RID_ID = "id";
    private static Logger logger = Logger.getLogger(IVFIndex.class.getName());

    public static final int NUM_DIMENSION, NUM_CENTROIDS, MAX_TRAINING_ITER, DIMENSION_DATA_UPPER_BOUND,
            MAX_TRAINING_TIME;

    static {
        NUM_DIMENSION = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".NUM_DIMENSIONS", 48);
        NUM_CENTROIDS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".NUM_CENTROIDS", 20);
        MAX_TRAINING_ITER = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".MAX_TRAINING_ITER", 20);
        DIMENSION_DATA_UPPER_BOUND = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".DIMENSION_DATA_UPPER_BOUND", 218);
        MAX_TRAINING_TIME = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".MAX_TRAINING_TIME", 1200);

    }

    // public static long searchCost(SearchKeyType keyType, long totRecs, long
    // matchRecs) {
    // int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
    // return (totRecs / rpb) / NUM_BUCKETS;
    // }

    private static String keyFieldName(int index) {
        return SCHEMA_KEY + index;
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
        sch.addField("centroid_num", INTEGER);
        return sch;
    }

    private static Schema data_schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        for (int i = 0; i < keyType.length(); i++)
            sch.addField(keyFieldName(i), keyType.get(i));
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }

    private static Schema temp_data_schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        for (int i = 0; i < keyType.length(); i++)
            sch.addField(keyFieldName(i), keyType.get(i));
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        sch.addField("centroid_num", INTEGER);
        return sch;
    }

    private SearchKey searchKey;
    private RecordFile rf;
    private boolean isBeforeFirsted;
    private Map<IntegerConstant, Constant> centroidMap;
    private Map<IntegerConstant, IntegerConstant> centDataNumMap;
    private long startTrainTime;
    private int num_items;

    /**
     * Opens a hash index for the specified index.
     * 
     * @param ii
     *                the information of this index
     * @param keyType
     *                the type of the search key
     * @param tx
     *                the calling transaction
     */
    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
    }

    // create centroid file and temp data file
    @Override
    public void Initialization() {
        close();

        TableInfo ti = new TableInfo(ii.indexName() + "_centroid", schema(keyType));
        rf = ti.open(tx, false);
        RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.close();

        TableInfo tempti = new TableInfo("_temp_" + ii.indexName() + "_data", temp_data_schema(keyType));
        RecordFile temprf = tempti.open(tx, false);
        RecordFile.formatFileHeader(tempti.fileName(), tx);
        temprf.close();

        tx.bufferMgr().flushAll();
    }

    void prepare_for_training() {
        centDataNumMap = new HashMap<IntegerConstant, IntegerConstant>();
        for (int i = 0; i < NUM_CENTROIDS; i++)
            centDataNumMap.put(new IntegerConstant(i), new IntegerConstant(0));
        num_items = 0;
        TableInfo tempti = new TableInfo("_temp_" + ii.indexName() + "_data", temp_data_schema(keyType));
        RecordFile temprf = tempti.open(tx, false);
        temprf.beforeFirst();
        while (temprf.next())
            num_items++;
        temprf.close();
    }

    // using kmeans to train the index
    @Override
    public void TrainIndex() {
        close();

        startTrainTime = System.currentTimeMillis();
        long prevTime = System.currentTimeMillis();

        prepare_for_training();

        int i = 1;
        while (i < MAX_TRAINING_ITER) {
            Map<IntegerConstant, IntegerConstant> oldCentDataNumMap = new HashMap<IntegerConstant, IntegerConstant>(
                    centDataNumMap);

            calculate_new_centroids();
            reassign_all_the_data();

            logger.info("After iteration " + String.valueOf(i) + ": \n" + print_cent_data_num_info(oldCentDataNumMap)
                    + "iteration " + String.valueOf(i) + ": "
                    + String.valueOf((System.currentTimeMillis() - prevTime) / 1000.0) + " seconds\n"
                    + "total elapsed time: " + String.valueOf((System.currentTimeMillis() - startTrainTime) / 1000.0)
                    + " seconds\n");

            // if the training converge then stop
            if ((any_change(oldCentDataNumMap) == false && i > 10)
                    || (System.currentTimeMillis() - startTrainTime > MAX_TRAINING_TIME * 1000))
                break;
            prevTime = System.currentTimeMillis();
            i++;
        }

        // write the trained centroids and data back to their files
        write_back_new_centroids();
        write_back_new_data();
    }

    private boolean any_change(Map<IntegerConstant, IntegerConstant> oldCentDataNumMap) {
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            if (oldCentDataNumMap.get(new IntegerConstant(i))
                    .equals(centDataNumMap.get(new IntegerConstant(i))) == false)
                return true;
        }
        return false;
    }

    private void calculate_new_centroids() {
        centroidMap = new HashMap<IntegerConstant, Constant>();
        centDataNumMap = new HashMap<IntegerConstant, IntegerConstant>();
        Map<Constant, Constant> centVcMap = new HashMap<Constant, Constant>();
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            centDataNumMap.put(new IntegerConstant(i), new IntegerConstant(0));
            centVcMap.put(new IntegerConstant(i), VectorConstant.zeros(NUM_DIMENSION));
        }

        TableInfo tempti = new TableInfo("_temp_" + ii.indexName() + "_data", temp_data_schema(keyType));
        RecordFile temprf = tempti.open(tx, false);
        temprf.beforeFirst();

        while (temprf.next()) {
            centVcMap.put(temprf.getVal("centroid_num"),
                    centVcMap.get(temprf.getVal("centroid_num")).add(temprf.getVal(keyFieldName(0))));
            centDataNumMap.put((IntegerConstant) temprf.getVal("centroid_num"),
                    (IntegerConstant) centDataNumMap.get(temprf.getVal("centroid_num"))
                            .add(new IntegerConstant(1)));
        }

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            if ((int) centDataNumMap.get(new IntegerConstant(i)).asJavaVal() == 0)
                centroidMap.put(new IntegerConstant(i), random_select_data_to_gen_vec(temprf));
            else
                centroidMap.put(new IntegerConstant(i),
                        centVcMap.get(new IntegerConstant(i)).div(centDataNumMap.get(new IntegerConstant(i))));
        }
        temprf.close();
    }

    private VectorConstant random_select_data_to_gen_vec(RecordFile temprf) {
        temprf.beforeFirst();
        Random rvg = new Random();
        int count = rvg.nextInt(num_items);
        for (int i = 0; i < count; i++)
            temprf.next();
        return (VectorConstant) temprf.getVal(keyFieldName(0));
    }

    private void reassign_all_the_data() {
        centDataNumMap = new HashMap<IntegerConstant, IntegerConstant>();
        for (int i = 0; i < NUM_CENTROIDS; i++)
            centDataNumMap.put(new IntegerConstant(i), new IntegerConstant(0));

        TableInfo tempti = new TableInfo("_temp_" + ii.indexName() + "_data", temp_data_schema(keyType));
        RecordFile temprf = tempti.open(tx, false);
        temprf.beforeFirst();
        while (temprf.next()) {
            int nearest_cent = calc_nearest_cent_num((VectorConstant) temprf.getVal(keyFieldName(0)));
            temprf.setVal("centroid_num", new IntegerConstant(nearest_cent));

            centDataNumMap.put(new IntegerConstant(nearest_cent), (IntegerConstant) centDataNumMap
                    .get(new IntegerConstant(nearest_cent)).add(new IntegerConstant(1)));

        }
        temprf.close();

        tx.bufferMgr().flushAll();
    }

    private void write_back_new_centroids() {
        close();
        TableInfo ti = new TableInfo(ii.indexName() + "_centroid", schema(keyType));
        rf = ti.open(tx, false);

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            rf.insert();
            rf.setVal(keyFieldName(0), centroidMap.get(new IntegerConstant(i)));
            rf.setVal("centroid_num", new IntegerConstant(i));
        }
        rf.close();
        tx.bufferMgr().flushAll();
    }

    private void write_back_new_data() {
        close();
        Map<IntegerConstant, RecordFile> centRfMap = new HashMap<IntegerConstant, RecordFile>();

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            TableInfo ti = new TableInfo(ii.indexName() + "_data_" + String.valueOf(i), data_schema(keyType));
            RecordFile datarf = ti.open(tx, false);
            RecordFile.formatFileHeader(ti.fileName(), tx);
            centRfMap.put(new IntegerConstant(i), datarf);
        }

        TableInfo tempti = new TableInfo("_temp_" + ii.indexName() + "_data", temp_data_schema(keyType));
        RecordFile temprf = tempti.open(tx, false);
        temprf.beforeFirst();

        while (temprf.next()) {
            RecordFile centRf = centRfMap.get(new IntegerConstant((int) temprf.getVal("centroid_num").asJavaVal()));
            centRf.insert();
            centRf.setVal(keyFieldName(0), temprf.getVal(keyFieldName(0)));
            centRf.setVal(SCHEMA_RID_BLOCK, temprf.getVal(SCHEMA_RID_BLOCK));
            centRf.setVal(SCHEMA_RID_ID, temprf.getVal(SCHEMA_RID_ID));
        }

        for (int i = 0; i < NUM_CENTROIDS; i++)
            centRfMap.get(new IntegerConstant(i)).close();
        temprf.remove();
        tx.bufferMgr().flushAll();
    }

    private String print_cent_data_num_info(Map<IntegerConstant, IntegerConstant> oldCentDataNumMap) {
        String s = "";
        for (int i = 0; i < NUM_CENTROIDS; i++) {
            s = s + "Centroid " + String.valueOf(i) + ": "
                    + String.valueOf(oldCentDataNumMap.get(new IntegerConstant(i)).asJavaVal()) + " -> "
                    + String.valueOf(centDataNumMap.get(new IntegerConstant(i)).asJavaVal()) + "\n";
        }
        return s;
    }

    @Override
    public void preLoadToMemory() {
        String tblname = ii.indexName() + "_centroid.tbl";
        long blk_size = fileSize(tblname);
        BlockId blk;
        for (int j = 0; j < blk_size; j++) {
            blk = new BlockId(tblname, j);
            tx.bufferMgr().pin(blk);
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

    }

    public void beforeFirst(SearchKey searchKey) {
        close();

        this.searchKey = searchKey;
        load_all_the_centroids();
        TableInfo ti = new TableInfo(ii.indexName() + "_data_" + String.valueOf(calc_nearest_cent_num(searchKey)),
                data_schema(keyType));
        rf = ti.open(tx, false);

        // initialize the file header if needed
        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);
        rf.beforeFirst();

        isBeforeFirsted = true;
    }

    private int calc_nearest_cent_num(SearchKey searchKey) {
        int smallestCentroidNum = 0;
        float minDistance = Float.MAX_VALUE;
        VectorConstant queryVector = (VectorConstant) searchKey.get(0);

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            VectorConstant centroid = (VectorConstant) centroidMap.get(new IntegerConstant(i));
            float distance = SIMDOperations.simdEuclideanDistance(queryVector.asJavaVal(), centroid.asJavaVal());
            if (distance < minDistance) {
                minDistance = distance;
                smallestCentroidNum = i;
            }
        }
        return smallestCentroidNum;
    }

    private int calc_nearest_cent_num(VectorConstant vc) {
        int smallestCentroidNum = 0;
        double minDistance = Float.MAX_VALUE;
        float[] queryVector = vc.asJavaVal();

        for (int i = 0; i < NUM_CENTROIDS; i++) {
            VectorConstant centroid = (VectorConstant) centroidMap.get(new IntegerConstant(i));
            float[] centroidVector = centroid.asJavaVal();
            float distance = SIMDOperations.simdEuclideanDistance(queryVector, centroidVector);

            if (distance < minDistance) {
                minDistance = distance;
                smallestCentroidNum = i;
            }
        }

        return smallestCentroidNum;
    }

    private void load_all_the_centroids() {
        close();
        centroidMap = new HashMap<IntegerConstant, Constant>();
        TableInfo ti = new TableInfo(ii.indexName() + "_centroid", schema(keyType));
        rf = ti.open(tx, false);
        rf.beforeFirst();
        while (rf.next())
            centroidMap.put((IntegerConstant) rf.getVal("centroid_num"), rf.getVal(keyFieldName(0)));
        rf.close();
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
        beforeFirst(key);

        // insert the data
        rf.insert();

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        for (int i = 0; i < keyType.length(); i++)
            rf.setVal(keyFieldName(i), key.get(i));
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
                .number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());
        rf.close();
    }

    public void insert_random(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        // random choose a centroid data file
        close();
        Random rvg = new Random();
        TableInfo tempti = new TableInfo("_temp_" + ii.indexName() + "_data", temp_data_schema(keyType));
        RecordFile temprf = tempti.open(tx, false);
        temprf.insert();

        // log the logical operation starts
        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        for (int i = 0; i < keyType.length(); i++)
            temprf.setVal(keyFieldName(i), key.get(i));
        temprf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
                .number()));
        temprf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
        temprf.setVal("centroid_num", new IntegerConstant(rvg.nextInt(NUM_CENTROIDS)));

        // log the logical operation ends
        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key,
                    dataRecordId.block().number(), dataRecordId.id());

        temprf.close();
    }

    /**
     * Deletes the specified index record.
     * 
     * @see Index#delete(SearchKey, RecordId, boolean)
     */
    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {

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

    public TableInfo getCentroidTableInfo() {
        return new TableInfo(ii.indexName() + "_centroid", schema(keyType));
    }

    public TableInfo getDataTableInfo(int i) {
        return new TableInfo(ii.indexName() + "_data_" + String.valueOf(i), data_schema(keyType));
    }
}