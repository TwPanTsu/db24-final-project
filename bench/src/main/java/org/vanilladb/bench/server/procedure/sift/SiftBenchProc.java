package org.vanilladb.bench.server.procedure.sift;

import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.*;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
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
    // add cluster here
    private static Cluster cluster;
    private static boolean haveCluster = false;
    private static int numOfCluster;
    // const value for top-k nearest centroid
    private static final int topKNC = 1;
    private DistanceFn distFn = new EuclideanFn("i_emb");

    private enum Strategy {
        NEAREST_CENTROID, TOP_K_NEAREST_CENTROID, TKNC_CONCURRENT
    }

    private Strategy strategy = Strategy.NEAREST_CENTROID;

    public SiftBenchProc() {
        super(new SiftBenchParamHelper());
    }

    @Override
    protected void executeSql() {

        SiftBenchParamHelper paramHelper = getHelper();
        Transaction tx = getTransaction();
        

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
            if(cluster.getNormOri()){
                String meanStandQuery = "SELECT mean , stand FROM mean_stand";
                Scan findMeanStand = StoredProcedureUtils.executeQuery(meanStandQuery, tx);
                VectorConstant mean = new VectorConstant(SiftBenchConstants.NUM_DIMENSION);
                VectorConstant  stand = new VectorConstant(SiftBenchConstants.NUM_DIMENSION);
                findMeanStand.beforeFirst();
                while (findMeanStand.next()) {
                    mean = (VectorConstant)findMeanStand.getVal("mean");
                    stand = (VectorConstant)findMeanStand.getVal("stand");
                }
                findMeanStand.close(); // make sure tx close
                cluster.setMeanStand(mean, stand);
            } 
            haveCluster = true;
        }
        /********************************************************************************** */

        VectorConstant query = paramHelper.getQuery();
        // set new query
        if (cluster.getNormOri()) query = cluster.normVector(query);
        if (cluster.getDimReduction()) query = cluster.reduceDim(query, SiftBenchConstants.NUM_DIMENSION);
        distFn.setQueryVector(query);

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

        } else if (strategy == Strategy.TOP_K_NEAREST_CENTROID) {
            // strategy 2: find top-k nearest centroids, then find nearest neighbor from
            // each cluster
            ArrayList<Integer> nearestCentroidIDs = cluster.getTopKNearestCentroidId(query, topKNC);
            PriorityQueue<CustomPair<Double, Integer>> pq = new PriorityQueue<CustomPair<Double, Integer>>(
                    paramHelper.getK(), new CustomPairComparator());
            for (int i = 0; i < topKNC; i++) {
                nnQuery = "SELECT i_id, i_emb FROM cluster_" + nearestCentroidIDs.get(i).toString() + " ORDER BY "
                        + paramHelper.getEmbeddingField() + " <EUC> " + query.toString() + " LIMIT "
                        + paramHelper.getK();

                Scan nearestNeighborScan = StoredProcedureUtils.executeQuery(nnQuery, tx);

                nearestNeighborScan.beforeFirst();
                while (nearestNeighborScan.next()) {
                    CustomPair<Integer, VectorConstant> candidate = new CustomPair<Integer, VectorConstant>(
                            (Integer) nearestNeighborScan.getVal("i_id").asJavaVal(),
                            (VectorConstant) nearestNeighborScan.getVal("i_emb"));
                    pq.add(new CustomPair<Double, Integer>(distFn.distance(candidate.getSecond()),
                            candidate.getFirst()));
                    if (pq.size() > paramHelper.getK()) {
                        pq.poll();
                    }
                }
                nearestNeighborScan.close();
            }

            Set<Integer> nearestNeighbors = pq.stream().map(CustomPair::getSecond).collect(HashSet::new, HashSet::add,
                    HashSet::addAll);
            paramHelper.setNearestNeighbors(nearestNeighbors);
        }
        /************************************************************************ */
        else {
            // ArrayList<Integer> nearestCentroidIDs = cluster.getTopKNearestCentroidId(query, topKNC);
            ArrayList<Integer> nearestCentroidIDs =
            cluster.getTopKNearestCentroidIdConcurrently(query, topKNC);
            PriorityQueue<CustomPair<Double, Integer>> pq = new PriorityQueue<CustomPair<Double, Integer>>(
                    paramHelper.getK(), new CustomPairComparator());
            List<Future<List<CustomPair<Double, Integer>>>> futures = new ArrayList<>();
            ExecutorService executor = Executors.newFixedThreadPool(topKNC * paramHelper.getK());
            for (int i = 0; i < topKNC; i++) {
                nnQuery = "SELECT i_id, i_emb FROM cluster_" + nearestCentroidIDs.get(i).toString() + " ORDER BY "
                        + paramHelper.getEmbeddingField() + " <EUC> " + query.toString() + " LIMIT "
                        + paramHelper.getK();

                Scan nearestNeighborScan = StoredProcedureUtils.executeQuery(nnQuery, tx);

                Callable<List<CustomPair<Double, Integer>>> task = new Callable<List<CustomPair<Double, Integer>>>() {
                    @Override
                    public List<CustomPair<Double, Integer>> call() throws Exception {
                        List<CustomPair<Double, Integer>> res = new ArrayList<>();
                        nearestNeighborScan.beforeFirst();
                        while (nearestNeighborScan.next()) {
                            CustomPair<Integer, VectorConstant> candidate = new CustomPair<Integer, VectorConstant>(
                                    (Integer) nearestNeighborScan.getVal("i_id").asJavaVal(),
                                    (VectorConstant) nearestNeighborScan.getVal("i_emb"));
                            res.add(new CustomPair<Double, Integer>(distFn.distance(candidate.getSecond()),
                                    candidate.getFirst()));
                        }
                        return res;
                    }
                };
                futures.add(executor.submit(task));

                // nearestNeighborScan.beforeFirst();
                // while (nearestNeighborScan.next()) {
                // CustomPair<Integer, VectorConstant> candidate = new CustomPair<Integer,
                // VectorConstant>(
                // (Integer) nearestNeighborScan.getVal("i_id").asJavaVal(),
                // (VectorConstant) nearestNeighborScan.getVal("i_emb"));
                // Callable<CustomPair<Double, Integer>> task = new Callable<CustomPair<Double,
                // Integer>>() {
                // @Override
                // public CustomPair<Double, Integer> call() throws Exception {
                // return new CustomPair<Double,
                // Integer>(distFn.distance(candidate.getSecond()),
                // candidate.getFirst());
                // }
                // };
                // futures.add(executor.submit(task));
                // }
            }
            executor.shutdown();
            try {
                for (Future<List<CustomPair<Double, Integer>>> future : futures) {
                    List<CustomPair<Double, Integer>> res = future.get();
                    for (CustomPair<Double, Integer> pair : res) {
                        pq.add(pair);
                        if (pq.size() > paramHelper.getK()) {
                            pq.poll();
                        }
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            Set<Integer> nearestNeighbors = pq.stream().map(CustomPair::getSecond).collect(HashSet::new, HashSet::add,
                    HashSet::addAll);
            paramHelper.setNearestNeighbors(nearestNeighbors);
        }
    }

}
