package org.vanilladb.bench.server.procedure.sift;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;

import org.vanilladb.bench.server.param.sift.SiftBenchParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.bench.util.CustomPair;
import org.vanilladb.bench.util.CustomPairComparator;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class SiftBenchProc extends StoredProcedure<SiftBenchParamHelper> {
    private static Logger logger = Logger.getLogger(SiftBenchProc.class.getName());
    // add cluster here
    private static Cluster cluster;
    private static boolean haveCluster = false;
    private static int numOfCluster;
    // const value for top-k nearest centroid
    private static final int topKNC = 2;
    private DistanceFn distFn = new EuclideanFn("i_emb");

    private enum Strategy {
        NEAREST_CENTROID, TOP_K_NEAREST_CENTROID
    }

    private Strategy strategy = Strategy.TOP_K_NEAREST_CENTROID;

    public SiftBenchProc() {
        super(new SiftBenchParamHelper());
    }

    @Override
    protected void executeSql() {

        SiftBenchParamHelper paramHelper = getHelper();
        VectorConstant query = paramHelper.getQuery();
        Transaction tx = getTransaction();
        distFn.setQueryVector(query);

        // build up the cluster for this class when the program first use the instance
        // of
        // this class
        if (!haveCluster) {
            String clusterQuery = "SELECT i_id , i_emb FROM " + paramHelper.getCentroidTableName();
            Scan findCluster = StoredProcedureUtils.executeQuery(clusterQuery, tx);
            ArrayList<float[]> centroids = new ArrayList<float[]>();
            findCluster.beforeFirst();
            while (findCluster.next()) {
                centroids.add((float[]) findCluster.getVal("i_emb").asJavaVal());
                // findCluster.getVal("i_emb").asJavaVal() is to find the centroid
                // vector(float[])
            }
            numOfCluster = centroids.size(); // to avoid synchronize
            findCluster.close(); // make sure tx close
            // new cluster here, just need the centroid varible in cluster here.
            cluster = new Cluster(centroids, numOfCluster);
            System.out.println("rebuilding cluster, cluster num = " + numOfCluster);
            haveCluster = true;
        }
        /********************************************************************************** */

        /*
         * String nnQuery = "SELECT i_id FROM " + paramHelper.getTableName() +
         * " ORDER BY " + paramHelper.getEmbeddingField() + " <EUC> " + query.toString()
         * + " LIMIT " + paramHelper.getK();
         */

        // our new query
        // ->first find nearest centroid, then find nearest neighbor from that cluster
        String nnQuery;
        if (strategy == Strategy.NEAREST_CENTROID) {
            // strategy 1: find nearest centroid, then find nearest neighbor from that
            // cluster
            int nearestCentroidID = cluster.getNearestCentroidId(query);
            // System.out.println("nearestCentroidId = " + nearestCentroidID);
            nnQuery = "SELECT i_id FROM cluster_" + nearestCentroidID +
                    " ORDER BY " + paramHelper.getEmbeddingField() + " <EUC> " + query.toString() + " LIMIT "
                    + paramHelper.getK();
            // Execute nearest neighbor search
            Scan nearestNeighborScan = StoredProcedureUtils.executeQuery(nnQuery, tx);

            nearestNeighborScan.beforeFirst();

            Set<Integer> nearestNeighbors = new HashSet<>();

            int count = 0;
            while (nearestNeighborScan.next()) {
                nearestNeighbors.add((Integer) nearestNeighborScan.getVal("i_id").asJavaVal());
                count++;
            }

            nearestNeighborScan.close();

            if (count == 0)
                throw new RuntimeException("Nearest neighbor query execution failed for " + query.toString());
            paramHelper.setNearestNeighbors(nearestNeighbors);

        } else {
            // strategy 2: find top-k nearest centroids, then find nearest neighbor from
            // each cluster
            ArrayList<Integer> nearestCentroidIDs = cluster.getTopKNearestCentroidId(query, topKNC);

            // debug
            // for (int i = 0; i < topKNC; i++) {
            // System.out.println("nearestCentroidId = " + nearestCentroidIDs.get(i));
            // }

            ArrayList<CustomPair<Integer, VectorConstant>> candidates = new ArrayList<CustomPair<Integer, VectorConstant>>();
            for (int i = 0; i < topKNC; i++) {
                nnQuery = "SELECT i_id, i_emb FROM cluster_" + nearestCentroidIDs.get(i).toString() + " ORDER BY "
                        + paramHelper.getEmbeddingField() + " <EUC> " + query.toString() + " LIMIT "
                        + paramHelper.getK();

                // logger.info(nnQuery);

                Scan nearestNeighborScan = StoredProcedureUtils.executeQuery(nnQuery, tx);

                nearestNeighborScan.beforeFirst();
                while (nearestNeighborScan.next()) {
                    candidates.add(new CustomPair<Integer, VectorConstant>(
                            (Integer) nearestNeighborScan.getVal("i_id").asJavaVal(),
                            (VectorConstant) nearestNeighborScan.getVal("i_emb")));
                }
                nearestNeighborScan.close();
            }

            // debug
            logger.info("candidates: ");
            for (CustomPair<Integer, VectorConstant> candidate: candidates) {
                logger.info(candidate.toString());
            }

            // Comparator<CustomPair<Double, Integer>> comparator = Comparator.comparing(CustomPair::getFirst);
            // PriorityQueue<CustomPair<Double, Integer>> pq = new PriorityQueue<>(comparator);
            PriorityQueue<CustomPair<Double, Integer>> pq = new PriorityQueue<CustomPair<Double, Integer>>(paramHelper.getK(), new CustomPairComparator());
            for (CustomPair<Integer, VectorConstant> candidate : candidates) {
                pq.add(new CustomPair<Double, Integer>(distFn.distance(candidate.getSecond()), candidate.getFirst()));
                if (pq.size() > paramHelper.getK()) {
                    pq.poll();
                }
            }

            // debug
            logger.info("pq:");
            logger.info(((Integer)pq.size()).toString());

            Set<Integer> nearestNeighbors = new HashSet<>();
            while (!pq.isEmpty()) {
                nearestNeighbors.add(pq.poll().getSecond());
            }

            // debug
            logger.info("set:");
            logger.info(((Integer)nearestNeighbors.size()).toString());

            // debug nearestNeighbors
            // for (Integer i : nearestNeighbors) {
            //     logger.info(i.toString());
            // }
            paramHelper.setNearestNeighbors(nearestNeighbors);
        }
        /************************************************************************ */

    }

}
