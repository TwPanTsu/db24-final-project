package org.vanilladb.core.storage.index.ivfflat;

import java.util.ArrayList;
import java.util.List;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.VECTOR;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class Cluster {

    private int cid;
    private VectorPair centroid;
    private List<VectorPair> vecs;
    private static final String SCHEMA_RID_ID = "id";
    private static final String SCHEMA_RID_BLK = "blk";
    private static final String SCHEMA_RID_VEC = "i_emb";
    private TableInfo ti;
    

    // Create new cluster when inserting
    public Cluster(RecordId rid, VectorConstant centroid, int cid, Transaction tx) {
        this.cid = cid;
        this.centroid = new VectorPair(centroid, rid.id(), rid.block().number());
        this.vecs = new ArrayList<>();
        this.vecs.add(this.centroid);

        String newClusterTable = "cluster_" + cid;
        VanillaDb.catalogMgr().createTable(newClusterTable, schema(centroid.dimension()), tx);
        ti = VanillaDb.catalogMgr().getTableInfo(newClusterTable, tx);
        RecordFile rf = ti.open(tx, false);
        rf.insert();
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(rid.id()));
        rf.setVal(SCHEMA_RID_BLK, new BigIntConstant(rid.block().number()));
        rf.setVal(SCHEMA_RID_VEC, centroid);
        rf.close();
    }

    // Load cluster from db
    public Cluster(int cid, Transaction tx) {
        this.vecs = new ArrayList<>();
        this.cid = cid;

        String newClusterTable = "cluster_" + cid;
        ti = VanillaDb.catalogMgr().getTableInfo(newClusterTable, tx);
        RecordFile rf = ti.open(tx, false);
        rf.beforeFirst();
        while (rf.next()) {
            IntegerConstant rid = (IntegerConstant) rf.getVal(SCHEMA_RID_ID);
            VectorConstant vec = (VectorConstant) rf.getVal(SCHEMA_RID_VEC);
            BigIntConstant blk = (BigIntConstant) rf.getVal(SCHEMA_RID_BLK);

            this.centroid = new VectorPair(vec, (Integer) rid.asJavaVal(), (Long) blk.asJavaVal());
            this.vecs.add(this.centroid);
        }
        rf.close();
    }

    public int getId() {
        return cid;
    }

    public int size() {
        return vecs.size();
    }

    public VectorConstant getCentroid() {
        return centroid.VectorConst;
    }

    public void setCentroid(VectorConstant centroid, RecordId rid) {
        this.centroid = new VectorPair(centroid, rid.id(),rid.block().number());
    }

    public List<VectorPair> getVecs() {
        return vecs;
    }

    public void addVec(VectorConstant vec, RecordId rid, Transaction tx) {
        vecs.add(new VectorPair(vec, rid.id(),rid.block().number()));
        RecordFile rf = ti.open(tx, false);
        rf.insert();
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(rid.id()));
        rf.setVal(SCHEMA_RID_BLK, new BigIntConstant(rid.block().number()));
        rf.setVal(SCHEMA_RID_VEC, vec);
        rf.close();
    }

    public void clearVecs() {
        vecs.clear();
    }

    private Schema schema(int dim) {
        Schema sch = new Schema();
        // for (int i = 0; i < keyType.length(); i++)
        sch.addField(SCHEMA_RID_ID, INTEGER);
        sch.addField(SCHEMA_RID_BLK, BIGINT);
        sch.addField(SCHEMA_RID_VEC, VECTOR(dim));

        return sch;
    }

    public VectorConstant calculateCentroid() {
        throw new UnsupportedOperationException("Unimplemented method 'Cluster::calculateCentroid'");
        // var new_arr = new float[centroid.VectorConst.dimension()];
        // for (VectorPair vec : vecs) {
        //     for (int i = 0; i < centroid.VectorConst.dimension(); i++) {
        //         new_arr[i] += vec.VectorConst.get(i);
        //     }
        // }
        // if (vecs.isEmpty()) {
        //     return null;
        // } else {
        //     int numPoints = vecs.size();
        //     for (int i = 0; i < centroid.VectorConst.dimension(); i++) {
        //         new_arr[i] /= numPoints;
        //     }
        //     return new VectorConstant(new_arr);
        // }
    }
}