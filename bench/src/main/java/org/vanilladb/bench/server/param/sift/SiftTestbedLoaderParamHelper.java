package org.vanilladb.bench.server.param.sift;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.bench.benchmarks.sift.SiftBenchConstants;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureHelper;

public class SiftTestbedLoaderParamHelper implements StoredProcedureHelper {

    private static final String TABLES_DDL[] = new String[1];
    private static final String INDEXES_DDL[] = new String[1];

    // dimension reduction parameter~!
    private static final boolean DIM_REDUCTION = false;
    private static final int N_DIM = (DIM_REDUCTION) ? (int)SiftBenchConstants.NUM_DIMENSION/2 : SiftBenchConstants.NUM_DIMENSION;

    // new values
    private static final String CENTROIDS_DDL[] = new String[1];
    private static final int numOfCluster = 1000; // hyperparameter that can be changed
    private static final String CLUSTERS_DDL[] = new String[numOfCluster];
    private static final String MEANSTAND_DDL[] = new String[1];
    

    private int numItems;

    public String getTableName() {
        return "sift";
    }

    public String getIdxName() {
        return "idx_sift";
    }

    public List<String> getIdxFields() {
        List<String> embFields = new ArrayList<String>(1);
        embFields.add("i_emb");
        return embFields;
    }

    public String[] getTableSchemas() {
        return TABLES_DDL;
    }

    public String[] getIndexSchemas() {
        return INDEXES_DDL;
    }

    public int getNumberOfItems() {
        return numItems;
    }

    public int getNumOfCluster() {
        return numOfCluster;
    }

    public String[] getCentroidSchemas(){
        return CENTROIDS_DDL;
    }

    public String[] getClusterSchemas(){
        return CLUSTERS_DDL;
    }

    public String[] getMeanStandSchemas(){
        return MEANSTAND_DDL;
    }

    public boolean getDimReduction(){
        return DIM_REDUCTION;
    }

    @Override
    public void prepareParameters(Object... pars) {
        numItems = (Integer) pars[0];
        // keep these the same for calculating recall?
        TABLES_DDL[0] = "CREATE TABLE " + getTableName() + " (i_id INT, i_emb VECTOR(" + SiftBenchConstants.NUM_DIMENSION + "))";
        INDEXES_DDL[0] = "CREATE INDEX " + getIdxName()+ " ON items (" + getIdxFields().get(0) + ") USING IVF";

        // our new tables
        CENTROIDS_DDL[0] = "CREATE TABLE centroids (i_id INT, i_emb VECTOR(" + N_DIM + "))";
        for (int i = 0; i < numOfCluster;i++)
            CLUSTERS_DDL[i] = "CREATE TABLE cluster_" + i + " (i_id INT, i_emb VECTOR(" + N_DIM + "))";
        
        if(DIM_REDUCTION) MEANSTAND_DDL[0] = "CREATE TABLE mean_stand (mean VECTOR(" + SiftBenchConstants.NUM_DIMENSION + "), stand VECTOR(" + SiftBenchConstants.NUM_DIMENSION + "))";
    }

    @Override
    public Schema getResultSetSchema() {
        return new Schema();
    }

    @Override
    public SpResultRecord newResultSetRecord() {
        return new SpResultRecord();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
