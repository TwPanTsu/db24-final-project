package org.vanilladb.core.query.algebra.vector;

import java.util.logging.Logger;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.ivfflat.IvfflatIndex;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborScan implements Scan {
    // private static Logger logger = Logger.getLogger(NearestNeighborScan.class.getName());

    private TableScan tableScan; 
    private Scan originalScan; 
    private Scan currentNeighborScan = null; 
    private Transaction transaction; 
    private IndexInfo indexInfo; 
    private DistanceFn distanceFunction;  
    private String temporaryTableName;  

    /**
     * Constructor to initialize NearestNeighborScan.
     * 
     * @param originalScan the original scan providing data points
     * @param transaction the transaction context
     * @param indexInfo the index information
     * @param distanceFunction the distance function for sorting
     */
    public NearestNeighborScan(Scan originalScan, Transaction transaction, IndexInfo indexInfo, DistanceFn distanceFunction) {
        this.tableScan = (TableScan) new TablePlan(indexInfo.tableName(), transaction).open();
        this.originalScan = originalScan;
        this.transaction = transaction;
        this.indexInfo = indexInfo;
        this.distanceFunction = distanceFunction;
    }

    /**
     * Positions the scan before the first record.
     */
    @Override
    public void beforeFirst() {
        tableScan.beforeFirst();
        originalScan.beforeFirst();
        initializeCurrentNeighborScan();
    }

    /**
     * Moves to the next record in the scan.
     * 
     * @return true if there is a next record, false otherwise
     */
    @Override
    public boolean next() {
        while (true) {
            if (currentNeighborScan == null) {
                return false;
            }
            if (currentNeighborScan.next()) {
                long blockNumber = (Long) currentNeighborScan.getVal(IvfflatIndex.SCHEMA_RID_BLOCK).asJavaVal();
                int recordId = (Integer) currentNeighborScan.getVal(IvfflatIndex.SCHEMA_RID_ID).asJavaVal();
                RecordId rid = new RecordId(new BlockId(indexInfo.tableName() + ".tbl", blockNumber), recordId);
                tableScan.moveToRecordId(rid);
                return true;
            } else {
                initializeCurrentNeighborScan();
            }
        }
    }

    /**
     * Closes the scan and releases any resources.
     */
    @Override
    public void close() {
        originalScan.close();
        tableScan.close();
        if (currentNeighborScan != null) {
            currentNeighborScan.close();
        }
    }

    /**
     * Checks if the specified field exists in the scan.
     * 
     * @param fieldName the name of the field
     * @return true if the field exists, false otherwise
     */
    @Override
    public boolean hasField(String fieldName) {
        return tableScan.hasField(fieldName);
    }

    /**
     * Retrieves the value of the specified field in the current record.
     * 
     * @param fieldName the name of the field
     * @return the value of the field
     */
    @Override
    public Constant getVal(String fieldName) {
        return tableScan.getVal(fieldName);
    }

    /**
     * Sets up the current neighbor scan by iterating through the original scan and 
     * preparing a sorted scan based on the distance function.
     */
    private void initializeCurrentNeighborScan() {
        while (originalScan.next()) {
            int index = (int) originalScan.getVal(IvfflatIndex.SCHEMA_ID).asJavaVal();
            temporaryTableName = indexInfo.indexName() + "-" + index;
            TableInfo tempTableInfo = VanillaDb.catalogMgr().getTableInfo(temporaryTableName, this.transaction);

            if (tempTableInfo == null) {
                continue;
            } else {
                Plan tablePlan = new TablePlan(temporaryTableName, this.transaction);
                tablePlan = new SortPlan(tablePlan, this.distanceFunction, this.transaction);
                if (currentNeighborScan != null) {
                    currentNeighborScan.close();
                }
                currentNeighborScan = tablePlan.open();
                currentNeighborScan.beforeFirst();
                return;
            }
        }
        currentNeighborScan = null;
    }
}
