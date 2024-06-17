package org.vanilladb.core.query.algebra.vector;

import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Logger;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.index.IndexType;
import org.vanilladb.core.storage.index.ivfflat.IvfflatIndex;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private static Logger logger = Logger.getLogger(NearestNeighborPlan.class.getName());
    private Plan childPlan;
    private IndexInfo indexInfo;
    private Transaction transaction;
    private int limit;
    private DistanceFn distanceFunction;

    /**
     * Constructs a NearestNeighborPlan.
     * 
     * @param tableName the name of the table
     * @param distanceFunction the distance function for sorting
     * @param transaction the transaction context
     * @param limit the maximum number of results to return
     */
    public NearestNeighborPlan(String tableName, DistanceFn distanceFunction, Transaction transaction, int limit) {
        this.limit = limit;
        this.transaction = transaction;
        List<IndexInfo> indexInfos = VanillaDb.catalogMgr().getIndexInfo(tableName, distanceFunction.fieldName(), transaction);
        this.indexInfo = null;

        // Find the appropriate index
        for (IndexInfo indexInfo : indexInfos) {
            if (indexInfo.indexType() == IndexType.IVFFLAT) {
                this.indexInfo = indexInfo;
                break;
            }
        }

        String indexTableName = this.indexInfo.indexName();
        Plan tablePlan = new TablePlan(indexTableName, transaction);
        this.distanceFunction = new EuclideanFn(IvfflatIndex.SCHEMA_KEY);
        this.distanceFunction.setQueryVector(distanceFunction.getQueryVector());

        if (limit > 50 || limit == -1) {
            this.childPlan = new SortPlan(tablePlan, this.distanceFunction, transaction);
        } else {
            this.childPlan = tablePlan;
        }
    }

    @Override
    public Scan open() {
        if (limit > 50 || limit == -1) {
            Scan scan = childPlan.open();
            return new NearestNeighborScan(scan, transaction, indexInfo, distanceFunction);
        } else {
            double minDistance = Double.MAX_VALUE;
            int minIndex = 0;
            Scan initialScan = childPlan.open();
            initialScan.beforeFirst();

            // Find the nearest neighbor
            while (initialScan.next()) {
                VectorConstant vector = (VectorConstant) initialScan.getVal(IvfflatIndex.SCHEMA_KEY);
                double distance = distanceFunction.distance(vector);
                if (distance < minDistance) {
                    minDistance = distance;
                    minIndex = (Integer) initialScan.getVal(IvfflatIndex.SCHEMA_ID).asJavaVal();
                }
            }
            initialScan.close();
            String indexTableName = indexInfo.indexName() + "-" + minIndex;
            return executeCalculateRecall(indexInfo.tableName(), indexTableName, distanceFunction, limit, transaction);
        }
    }

    @Override
    public long blocksAccessed() {
        return childPlan.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return childPlan.schema();
    }

    @Override
    public Histogram histogram() {
        return childPlan.histogram();
    }

    @Override
    public long recordsOutput() {
        return childPlan.recordsOutput();
    }

    // Inner class to represent a record as a map of field values
    static class MapRecord implements Record {
        private Map<String, Constant> fieldValues = new HashMap<>();

        @Override
        public Constant getVal(String fieldName) {
            return fieldValues.get(fieldName);
        }

        public void put(String fieldName, Constant value) {
            fieldValues.put(fieldName, value);
        }

        public boolean containsKey(String fieldName) {
            return fieldValues.containsKey(fieldName);
        }
    }

    // Executes the nearest neighbor search and returns a scan of the results
    public static Scan executeCalculateRecall(String originalTableName, String indexTableName, DistanceFn distanceFunction, int limit, Transaction transaction) {
        Plan plan = new TablePlan(indexTableName, transaction);

        List<String> sortFields = new ArrayList<>();
        sortFields.add(distanceFunction.fieldName());

        List<Integer> sortDirections = new ArrayList<>();
        sortDirections.add(DIR_DESC); // For priority queue

        RecordComparator comparator = new RecordComparator(sortFields, sortDirections, distanceFunction);

        PriorityQueue<MapRecord> priorityQueue = new PriorityQueue<>(limit, (MapRecord r1, MapRecord r2) -> comparator.compare(r1, r2));

        Scan scan = plan.open();
        scan.beforeFirst();
        while (scan.next()) {
            MapRecord fieldValues = new MapRecord();
            for (String fieldName : plan.schema().fields()) {
                fieldValues.put(fieldName, scan.getVal(fieldName));
            }
            priorityQueue.add(fieldValues);
            if (priorityQueue.size() > limit) {
                priorityQueue.poll();
            }
        }
        scan.close();

        return new NearestNeighborUtils.PriorityQueueScan(priorityQueue, originalTableName, transaction);
    }
}
