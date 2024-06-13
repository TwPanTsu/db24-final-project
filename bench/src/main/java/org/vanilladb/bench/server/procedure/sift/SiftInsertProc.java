package org.vanilladb.bench.server.procedure.sift;

import java.util.ArrayList;

import org.vanilladb.bench.server.param.sift.SiftInsertParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureUtils;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.tx.Transaction;

public class SiftInsertProc extends StoredProcedure<SiftInsertParamHelper> {

    // add cluster here
    private static Cluster cluster;
    private static boolean haveCluster = false;
    private static int numOfCluster = 0;

    public SiftInsertProc() {
        super(new SiftInsertParamHelper());
    }

    @Override
    protected void executeSql() {
        SiftInsertParamHelper paramHelper = getHelper();
        Transaction tx = getTransaction();

        // build up the cluster for this class when the program first use the instance of 
        //  this class
        if(!haveCluster){
            String clusterQuery = "SELECT i_id , i_emb FROM " + paramHelper.getCentroidTableName();
            Scan findCluster = StoredProcedureUtils.executeQuery(clusterQuery, tx);
            ArrayList<float[]> centroids = new ArrayList<float[]>();
            findCluster.beforeFirst();
            while (findCluster.next()) {
                centroids.add((float[]) findCluster.getVal("i_emb").asJavaVal());
                //findCluster.getVal("i_emb").asJavaVal() is to find the centroid vector(float[])
            }
            numOfCluster = centroids.size(); // to avoid synchronize 
            findCluster.close(); // make sure tx close
            // new cluster here, just need the centroid varible in cluster here.
            cluster = new Cluster(centroids, numOfCluster);
            System.out.println("rebuilding cluster, cluster num = " + numOfCluster);
            haveCluster = true;
        }
        /********************************************************************************** */

        VectorConstant v = paramHelper.getNewVector();

        //String sql = "INSERT INTO sift(i_id, i_emb) VALUES (" + paramHelper.getId() + ", " + v.toString() + ")";
        
        // our new query
        //  ->first find nearest centroid, then insert the vector to that cluster
        int nearestCentroidID = cluster.getNearestCentroidId(v);
        String sql = "INSERT INTO cluster_" + nearestCentroidID + "(i_id, i_emb) VALUES (" 
            + paramHelper.getId() + ", " + v.toString() + ")";

        /************************************************************************** */

        StoredProcedureUtils.executeUpdate(sql, tx);
    }   
}
