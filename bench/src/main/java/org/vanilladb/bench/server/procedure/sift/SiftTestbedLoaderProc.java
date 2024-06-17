package org.vanilladb.bench.server.procedure.sift;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.server.param.sift.SiftTestbedLoaderParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.bench.util.BenchProperties;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.storage.index.ivf.IVFIndex;

public class SiftTestbedLoaderProc extends StoredProcedure<SiftTestbedLoaderParamHelper> {
    private static Logger logger = Logger.getLogger(SiftTestbedLoaderProc.class.getName());
    private static int NUM_CLUSTERS;
    private static int num_items;
    private static int batch_size;
    static {
		NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(
				IVFIndex.class.getName() + ".NUM_CLUSTERS", 9);
        num_items = BenchProperties.getLoader().getPropertyAsInteger(
                SiftBenchConstants.class.getName() + ".NUM_ITEMS", 900);
        batch_size = num_items / 90;
	}
    public SiftTestbedLoaderProc() {
        super(new SiftTestbedLoaderParamHelper());
    }

    @Override
    protected void executeSql() {
        if (logger.isLoggable(Level.INFO))
            logger.info("Start loading testbed...");
        logger.info("Trying to call kmeans.py");

        try {
            // 定義 Python 腳本的路徑
            String currDir = System.getProperty("user.dir");
            String pythonScriptPath = currDir + File.separator + "kmeans.py";
            System.out.println(pythonScriptPath);

            String outputDir = currDir + File.separator + "clusters_output";
            System.out.println(outputDir);
            // 創建一個 ProcessBuilder 來運行 Python 腳本
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "python", pythonScriptPath,
                    "--num_items", String.valueOf(num_items),
                    "--n_clusters", String.valueOf(NUM_CLUSTERS),
                    "--batch_size", String.valueOf(batch_size),
                    "--output_dir", outputDir);

            // 啟動過程
            Process process = processBuilder.start();

            // 讀取 Python 腳本的輸出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // 等待 Python 腳本執行完畢
            int exitCode = process.waitFor();
            System.out.println("Python script exited with code: " + exitCode);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // turn off logging set value to speed up loading process
        RecoveryMgr.enableLogging(false);

        dropOldData();
        createSchemas();

        // Generate item records
        generateItems(0);

        if (logger.isLoggable(Level.INFO))
            logger.info("Training IVF index...");
        // 建 Index 要用的 table
        executeTrainIndex(getHelper().getTableName(), getHelper().getIdxFields(),
                getHelper().getIdxName(), getTransaction());

        if (logger.isLoggable(Level.INFO))
            logger.info("Loading completed. Flush all loading data to disks...");

        RecoveryMgr.enableLogging(true);

        // Create a checkpoint
        CheckpointTask cpt = new CheckpointTask();
        cpt.createCheckpoint();

        // Delete the log file and create a new one
        VanillaDb.logMgr().removeAndCreateNewLog();

        if (logger.isLoggable(Level.INFO))
            logger.info("Loading procedure finished.");
    }

    private void dropOldData() {
        if (logger.isLoggable(Level.WARNING))
            logger.warning("Dropping is skipped.");
    }

    private void createSchemas() {
        SiftTestbedLoaderParamHelper paramHelper = getHelper();
        Transaction tx = getTransaction();

        if (logger.isLoggable(Level.FINE))
            logger.info("Creating tables...");

        for (String sql : paramHelper.getTableSchemas())
            StoredProcedureUtils.executeUpdate(sql, tx);

        if (logger.isLoggable(Level.INFO))
            logger.info("Creating indexes...");

        // Create indexes
        for (String sql : paramHelper.getIndexSchemas())
            StoredProcedureUtils.executeUpdate(sql, tx);

        if (logger.isLoggable(Level.FINE))
            logger.info("Finish creating schemas.");
    }

    private void generateItems(int startIId) {
        if (logger.isLoggable(Level.FINE))
            logger.info("Start populating items from SIFT1M dataset");

        Transaction tx = getTransaction();

        try (BufferedReader br = new BufferedReader(new FileReader(SiftBenchConstants.DATASET_FILE))) {
            int iid = startIId;
            String vectorString;

            while (iid < SiftBenchConstants.NUM_ITEMS && (vectorString = br.readLine()) != null) {
                String sql = "INSERT INTO sift(i_id, i_emb) VALUES (" + iid + ", [" + vectorString + "])";
                // logger.info(sql);
                iid++;
                StoredProcedureUtils.executeUpdate(sql, tx);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (logger.isLoggable(Level.FINE))
            logger.info("Finish populating items.");
    }
    // 根據 KNN 跑出來的 txt 內容創建 centroids 和 clusters 的 table
    public void executeTrainIndex(String tblname, List<String> fldnames, String IdxName, Transaction tx) {
        Set<IndexInfo> indexes = new HashSet<IndexInfo>();

        int clusterNum = NUM_CLUSTERS;
        int itemInClusterNum = SiftBenchConstants.NUM_ITEMS / clusterNum;

		// 找找看在 DB 存的 IndexInfo 有沒有符合這個 tableName 和 FieldName 的。
        List<IndexInfo> iis = VanillaDb.catalogMgr().getIndexInfo(tblname, fldnames.get(0), tx);
        IndexInfo ii = iis.get(0);
        IVFIndex idx = (IVFIndex) ii.open(tx);

        // 插入 centroids
        List<Int8VectorConstant> vectorList = new ArrayList<Int8VectorConstant>();
        try (BufferedReader br = new BufferedReader(new FileReader("clusters_output\\centroids.txt"))) {
            int line = 0;
            String vectorString;

            while (line < clusterNum && (vectorString = br.readLine()) != null) {
                vectorList.add(new Int8VectorConstant(vectorString));
                line++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        idx.createCentroidTable(vectorList);

        // 插入 clusters
        for (int i = 0; i < clusterNum; i++) {
            List<Int8VectorConstant> vectorList_cluster = new ArrayList<Int8VectorConstant>();
            List<IntegerConstant> intList_cluster = new ArrayList<IntegerConstant>();
            try (BufferedReader brVec = new BufferedReader(new FileReader("clusters_output\\cluster_"+i+".txt"))) {
                try (BufferedReader brIdx = new BufferedReader(new FileReader("clusters_output\\cluster_"+i+"_indices.txt"))) {

                    String vectorString;
                    String id;
                    while ((vectorString = brVec.readLine()) != null && (id = brIdx.readLine())!= null) {
                        vectorList_cluster.add(new Int8VectorConstant(vectorString));
                        intList_cluster.add(new IntegerConstant(Integer.parseInt(id)));
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            idx.createClusterTable(intList_cluster, vectorList_cluster, i);

        }
    }
}
