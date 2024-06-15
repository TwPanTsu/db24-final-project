package org.vanilladb.bench.server.procedure.sift;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.bench.server.param.sift.SiftTestbedLoaderParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class SiftTestbedLoaderProc extends StoredProcedure<SiftTestbedLoaderParamHelper> {
    private static Logger logger = Logger.getLogger(SiftTestbedLoaderProc.class.getName());
    

    public SiftTestbedLoaderProc() {
        super(new SiftTestbedLoaderParamHelper());
    }

    @Override
    protected void executeSql() {
        if (logger.isLoggable(Level.INFO))
            logger.info("Start loading testbed...");

        // turn off logging set value to speed up loading process
        RecoveryMgr.enableLogging(false);

        dropOldData();
        createSchemas();

        // Generate item records
        generateItems(0);

        // if (logger.isLoggable(Level.INFO))
        //     logger.info("Training IVF index...");

        // StoredProcedureUtils.executeTrainIndex(getHelper().getTableName(), getHelper().getIdxFields(), 
        //     getHelper().getIdxName(), getTransaction());

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

        // Create our new tables
        for (String sql : paramHelper.getCentroidSchemas())
            StoredProcedureUtils.executeUpdate(sql, tx);

        for (String sql : paramHelper.getClusterSchemas())
            StoredProcedureUtils.executeUpdate(sql, tx);

        // build mean, standard deviation table
        if(paramHelper.getNormOri()){
            for (String sql : paramHelper.getMeanStandSchemas())
                StoredProcedureUtils.executeUpdate(sql, tx);
        }

        // if (logger.isLoggable(Level.INFO))
        //     logger.info("Creating indexes...");

        // // Create indexes
        // for (String sql : paramHelper.getIndexSchemas())
        //     StoredProcedureUtils.executeUpdate(sql, tx);
        
        if (logger.isLoggable(Level.FINE))
            logger.info("Finish creating schemas.");
    }

    private void generateItems(int startIId) {
        if (logger.isLoggable(Level.FINE))
            logger.info("Start populating items from SIFT1M dataset");

        // Create cluster here
        SiftTestbedLoaderParamHelper paramHelper = getHelper();
        int numOfCluster = paramHelper.getNumOfCluster();
        Cluster cluster = new Cluster(numOfCluster);
        cluster.clustering(50);// numOfRound is hyperparameter that can be chamged

        Transaction tx = getTransaction();

        // Insert Mean and standard diveation data
        if(paramHelper.getDimReduction() || paramHelper.getNormOri()){
            String sql = "INSERT INTO mean_stand(mean, stand) VALUES (" + cluster.meanOfAllDIM.toString() + ", " + cluster.standOfAllDIM.toString() + ")";
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        //Insert the value of each centroid to centroid table
        for (int i = 0; i < numOfCluster;i++){
            float[] centroid_vec = cluster.getCentroid().get(i);
            VectorConstant tempVC = new VectorConstant(centroid_vec);
            String vectorString = tempVC.toString(); // make it become ex: [1.5, 2.0, ...]
            String sql = "INSERT INTO centroids(i_id, i_emb) VALUES (" + i + ", " + vectorString + ")";
            //System.out.println(sql+", len = "+ tempVC.dimension());
            StoredProcedureUtils.executeUpdate(sql, tx);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(SiftBenchConstants.DATASET_FILE))) {
            int iid = startIId;
            String vectorString;

            while (iid < SiftBenchConstants.NUM_ITEMS && (vectorString = br.readLine()) != null) {
                String sql = "INSERT INTO sift(i_id, i_emb) VALUES (" + iid + ", [" + vectorString + "])";
                // logger.info(sql);
                
                StoredProcedureUtils.executeUpdate(sql, tx);

                // insert the record to its cluster
                String this_sql;
                if(cluster.getDimReduction() && cluster.getNormOri()){
                    VectorConstant reducedAndNormVec = cluster.stringToVectorWithReductionAndNorm(vectorString, SiftBenchConstants.NUM_DIMENSION);
                    int centroid_id = cluster.getNearestCentroidId(reducedAndNormVec);
                    this_sql = "INSERT INTO cluster_"+ centroid_id +"(i_id, i_emb) VALUES (" + iid + ", " + reducedAndNormVec.toString() + ")";
                    //System.out.println(this_sql+", len = "+ reducedVec.dimension());
                } else if (cluster.getDimReduction()){// just reduction
                    VectorConstant reducedVec = cluster.stringToVectorWithReduction(vectorString, SiftBenchConstants.NUM_DIMENSION);
                    int centroid_id = cluster.getNearestCentroidId(reducedVec);
                    this_sql = "INSERT INTO cluster_"+ centroid_id +"(i_id, i_emb) VALUES (" + iid + ", " + reducedVec.toString() + ")";
                } else if (cluster.getNormOri()){//just norm
                    VectorConstant normVec = new VectorConstant(cluster.stringToVector(vectorString));
                    normVec = cluster.normVector(normVec);
                    int centroid_id = cluster.getNearestCentroidId(normVec);
                    this_sql = "INSERT INTO cluster_"+ centroid_id +"(i_id, i_emb) VALUES (" + iid + ", " + normVec.toString() + ")";
                }else {
                    int centroid_id = cluster.getNearestCentroidId(vectorString);
                    this_sql = "INSERT INTO cluster_"+ centroid_id +"(i_id, i_emb) VALUES (" + iid + ", [" + vectorString + "])";
                }
                StoredProcedureUtils.executeUpdate(this_sql, tx);
                iid++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (logger.isLoggable(Level.FINE))
            logger.info("Finish populating items.");
    }
}
