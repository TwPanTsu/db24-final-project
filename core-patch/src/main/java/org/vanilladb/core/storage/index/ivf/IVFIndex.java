package org.vanilladb.core.storage.index.ivf;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.storage.metadata.TableInfo;
import java.util.logging.Logger;
import java.util.List;
// Final

class Pair implements Comparable<Pair> {
    public Double dist;
    public int id;

    Pair(Double dist, int id) {
        this.dist = dist;
        this.id = id;
    }

    @Override
    public int compareTo(Pair other) {
        return this.dist.compareTo(other.dist);
    }
}

class DataPair implements Comparable<Pair> {
    public VectorConstant vec;
    public int id;

    DataPair(VectorConstant vec, int id) {
        this.vec = vec;
        this.id = id;
    }

    @Override
    public int compareTo(Pair other) {
        return (this.id > other.id)? 1:0;
    }
}
public class IVFIndex { 
    private static Logger logger = Logger.getLogger(VanillaDb.class.getName());
    private int num_dim = CoreProperties.getLoader().getPropertyAsInteger(VanillaDb.class.getName() + ".NUM_DIMENSIONS", 128);
    private String embField = "i_emb";
    private String table_name = "items";
    private int num_neighbors = 20; 
    private int num_groups = 500; 
    private int update_cnt = 0;
    private int update_threshold = 5000;
    private boolean is_clustered = false;
    private double max_error_threshold = 128;
    private DataPair data_to_update;
    private ArrayList<VectorConstant> centroid_vec;
    private ArrayList<ArrayList<DataPair>> cluster_files;
    private static Object createTableLock = new Object();
    private boolean isInited = false;
    Transaction tx;

    public IVFIndex() {
        logger.info("Initialize IVFIndex.");
        tx = VanillaDb.txMgr().newTransaction(
							Connection.TRANSACTION_SERIALIZABLE, false);  
        logger.info("Finish IVFIndex.");


        centroid_vec = new ArrayList<VectorConstant>();
        for(int i = 0; i < num_groups; i++) {
            float[] init_c = new float[num_dim];
            for(int j = 0; j < num_dim; j++)
                init_c[j] = (float) (0.5 * i);
            VectorConstant random_c = new VectorConstant(init_c);
            centroid_vec.add(random_c);
        }
        logger.info("create centroid table");

        cluster_files = new ArrayList<ArrayList<DataPair>>();
        for(int i = 0; i < num_groups; i++) {
            cluster_files.add(new ArrayList<DataPair>());
        }
        logger.info("create cluster table");
	}

    // public void createTable() {
    //     // Create centroid file.
    //     //Schema centroid_file_sch = new Schema();
    //     //centroid_file_sch.addField("centroid_idx", Type.INTEGER);
    //     //centroid_file_sch.addField("centroid_vec", Type.VECTOR(num_dim));
    //     //VanillaDb.catalogMgr().createTable("centroid", centroid_file_sch, tx);
    //     //String sql = "CREATE TABLE centroid (centroid_idx INT, centroid_vec VECTOR(128))";
    //     //VanillaDb.newPlanner().executeUpdate(sql, tx);

    //     //logger.info("1");

    //     centroid_vec = new ArrayList<VectorConstant>();
    //     for(int i = 0; i < num_groups; i++) {
    //         VectorConstant random_c = new VectorConstant(num_dim);
    //         centroid_vec.add(random_c);
    //     }

    //     logger.info("create centroid table");

    //     // Create Cluster files.
    //     // Schema cluster_files_sch = new Schema();
    //     // cluster_files_sch.addField(embField, Type.VECTOR(num_dim));
    //     // cluster_files_sch.addField("i_id",Type.INTEGER);
    //     // for (int i = 0; i < num_groups; i++) {
    //     //     VanillaDb.catalogMgr().createTable("cluster" + i, cluster_files_sch, tx);
    //     // }
    //     cluster_files = new ArrayList<ArrayList<DataPair>>();
    //     for(int i = 0; i < num_groups; i++) {
    //         cluster_files.add(new ArrayList<DataPair>());
    //     }

    //     logger.info("create cluster table");
    // }

    public int count_num_datas(){
        int cnt=0;
        for(ArrayList<DataPair> arr: cluster_files) {
            for(DataPair dp: arr){
                cnt++;
            }
        }
        return cnt;
    }

    private void loadClusterFromFile(){
        TableInfo ti = VanillaDb.catalogMgr().getTableInfo("sift", tx);
        RecordFile rf = ti.open(tx, false);
        rf.beforeFirst();
        while(rf.next()) {
            int id = (int)rf.getVal("i_id").asJavaVal();
            DoubleConstant iid = new DoubleConstant((double)id);
            Constant vec = rf.getVal("i_emb");
            List<Constant> val =  Arrays.asList(iid, vec);
            InsertData data_from_db = new InsertData("sift", null, val);
            UpdateCluster(data_from_db, tx);
        }
        rf.close();
    }

    synchronized public List<Constant> find_cluster(VectorConstant vec, Transaction tx) {
        if(!isInited){
            loadClusterFromFile();
            isInited = true;
        }
        //logger.info("In find_cluster.");
        DistanceFn distFn = new EuclideanFn("vector");
		distFn.setQueryVector(vec);
        //logger.info("vec" + vec);

        // 1. Find the 5 centroids nearest to vec

        ArrayList<Pair> centroids_pairs = new ArrayList<Pair>();
        for (int i = 0; i < num_groups; i++) {
            //logger.info("dis: "+distFn.distance(centroid_vec.get(i)));
            centroids_pairs.add(new Pair(distFn.distance(centroid_vec.get(i)), i));
        }
        Collections.sort(centroids_pairs);

        //logger.info("5 most nearest centroid:");
        //for(int i=0; i<5; i++)
        //    logger.info(""+centroids_pairs.get(i).id+" "+centroids_pairs.get(i).dist);
        
        // 2. Calculate distance between query vector and all other vectors

        ArrayList<Pair> vector_pairs = new ArrayList<Pair>();

       
        for (int i=0; i<3 || vector_pairs.size()<num_neighbors; i++) {
            //logger.info("num group: " + i);
            int centroid_id = centroids_pairs.get(i).id;
            /*TableInfo ti = VanillaDb.catalogMgr().getTableInfo(table_name + "cluster" + centroid_id, tx);
            RecordFile rf = ti.open(tx, false);
            rf.beforeFirst();

            while (rf.next()) {
                Constant v = rf.getVal(embField);
                Constant id = rf.getVal("i_id");
                VectorConstant vector = new VectorConstant((float[])v.asJavaVal());
                Double dist = distFn.distance(vector);

                vector_pairs.add(new Pair(dist, (int)id.asJavaVal()));
            }*/
            ArrayList<DataPair> temp = cluster_files.get(centroid_id);
            //logger.info("temp size :" + temp.size());
            for(int j = 0; j < temp.size(); j++) {
                Double dist = distFn.distance(temp.get(j).vec);
                vector_pairs.add(new Pair(dist, temp.get(j).id));
            }
        }
        Collections.sort(vector_pairs);
        logger.info("vector_pairs size :" + vector_pairs.size());
        // 3. Find 20 nearest neighborhood

        List<Constant> nn = new ArrayList<Constant>();
        int min_size = num_neighbors;
        //if(min_size > vector_pairs.size())
        //    min_size = vector_pairs.size();
        for (int i=0; i< min_size; i++) {
            Constant c = new IntegerConstant(vector_pairs.get(i).id);
            nn.add(c);
            //nn.add(new IntegerConstant(i));
            //logger.info("add :" + c);
        }
        logger.info("nn size :" + nn.size());
        return nn;
    }

    int find_nearest_cluster(VectorConstant vec) {
        
        DistanceFn distFn = new EuclideanFn("vector");
		distFn.setQueryVector(vec);

        Double min_dist = Double.MAX_VALUE;
        int min_clusterId = 0;

        for (int i = 0; i < num_groups; i++) {
            Double dist = distFn.distance(centroid_vec.get(i));
            if(dist < min_dist) {
                min_dist = dist;
                min_clusterId = i;
            }
        }
        
        return min_clusterId;
    }


    synchronized public void UpdateCluster(InsertData data, Transaction tx) {
        update_cnt++;
        double d = (double)data.vals().get(0).asJavaVal();
        data_to_update = new DataPair((VectorConstant) data.vals().get(1), (int)d);

        int min_clusterId = find_nearest_cluster(data_to_update.vec);

        ArrayList<DataPair> cluster_content = cluster_files.get(min_clusterId);
        cluster_content.add(data_to_update);
        cluster_files.set(min_clusterId, cluster_content);
        // TableInfo ti = VanillaDb.catalogMgr().getTableInfo(table_name + "cluster" + min_clusterId, tx);
        // RecordFile rf = ti.open(tx, true);
        // logger.info(ti.fileName());

        // rf.insert();
        //logger.info("In UpdateCluster 3.");
        //rf.beforeFirst();
        //logger.info("In UpdateCluster 4.");
        //rf.next();
        // rf.setVal(embField, data_to_update.vec);
        // rf.setVal("i_id", new IntegerConstant(data_to_update.id));

        // rf.close();

        // When exceed 4000 updates.
        boolean flag = true;
        if(!is_clustered) {
            update_threshold = 250000-1;
        } else {
            update_threshold = 50000000;
        }
        if(update_cnt >= update_threshold) {
            update_cnt = 0;
            is_clustered = true;
            while(flag) {
                // Update Each groups center.
                flag = false; // Assume no need to recluster.
                for(int i = 0; i < num_groups; i++) {
                    int num_of_nodes = 1;
                    VectorConstant new_centroid = VectorConstant.zeros(num_dim);
                    // TableInfo ti_2 = VanillaDb.catalogMgr().getTableInfo(table_name + "cluster" + i, tx);
                    // RecordFile rf_2 = ti_2.open(tx, false);
                    // rf_2.beforeFirst();
                    // while(rf_2.next()) {
                    //     num_of_cluster++;
                    //     new_centroid.add(rf_2.getVal(embField));
                    // }
                    ArrayList<DataPair>cluster_nodes = cluster_files.get(i);
                    for(DataPair node: cluster_nodes){
                        num_of_nodes++;
                        new_centroid = (VectorConstant)new_centroid.add(node.vec);
                        //logger.info("node.vec: " + node.vec + "new" + new_centroid);
                    }
                    new_centroid = (VectorConstant) new_centroid.div(new IntegerConstant(num_of_nodes));
                    //logger.info("new" + new_centroid);
                    //if(num_of_nodes !=0) logger.info("cluster"+i+"has nodes: " + num_of_nodes);

                    DistanceFn distFn = new EuclideanFn("vector");
                    distFn.setQueryVector(new_centroid);
                    if(distFn.distance(centroid_vec.get(i)) > max_error_threshold) {
                        flag = true;
                        centroid_vec.set(i, new_centroid);
                    }
                    //rf_2.close();
                }
                // Re-cluster each data.
                for(int i = 0; i < num_groups; i++) {
                    // TableInfo ti_2 = VanillaDb.catalogMgr().getTableInfo(table_name + "cluster" + i, tx);
                    // RecordFile rf_2 = ti_2.open(tx, false);
                    // rf_2.beforeFirst();
                    // while(rf_2.next()) {
                    //     Constant id = rf_2.getVal("i_id");
                    //     VectorConstant vec = (VectorConstant)rf_2.getVal(embField);
                    //     int new_cluster = find_nearest_cluster(vec);

                    //     if(new_cluster != i) {
                    //         rf_2.delete();

                    //         TableInfo ti_3 = VanillaDb.catalogMgr().getTableInfo(table_name + "cluster" + new_cluster, tx);
                    //         RecordFile rf_3 = ti_3.open(tx, false);
                    //         rf_3.insert();
                    //         //rf_3.beforeFirst();
                    //         //rf_3.next();
                    //         rf_3.setVal(embField, vec);
                    //         rf_3.setVal("i_id", id);
                    //         rf_3.close();
                    //     }
                    // }
                    // rf_2.close();
                    ArrayList<DataPair>cluster_nodes = cluster_files.get(i);
                    ArrayList<DataPair>cluster_nodes_ = new ArrayList<DataPair>(cluster_nodes);
                    for(DataPair node: cluster_nodes){
                        int new_cluster = find_nearest_cluster(node.vec);
                        if(new_cluster != i) {
                            cluster_nodes_.remove(node);

                            ArrayList<DataPair> new_cluster_nodes = cluster_files.get(new_cluster);
                            new_cluster_nodes.add(node);
                            cluster_files.set(new_cluster, new_cluster_nodes);
                        }
                    }
                    cluster_files.set(i, cluster_nodes_);
                }

            }
        }

    }

    // public void remove(){

    // }
}
