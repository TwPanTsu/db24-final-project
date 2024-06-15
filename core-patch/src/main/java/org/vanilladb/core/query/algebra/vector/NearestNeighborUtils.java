package org.vanilladb.core.query.algebra.vector;

import java.util.PriorityQueue;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.vector.NearestNeighborPlan.MapRecord;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.ivfflat.IvfflatIndex;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborUtils {
    // Scan implementation using a priority queue
    // modified from storedProcedureUtils.java
    static class PriorityQueueScan implements Scan {
        private PriorityQueue<MapRecord> priorityQueue;
        private boolean isBeforeFirst = false;
        private TableScan tableScan;
        private Transaction transaction;
        private String originalTableName;

        public PriorityQueueScan(PriorityQueue<MapRecord> priorityQueue, String originalTableName, Transaction transaction) {
            this.priorityQueue = priorityQueue;
            this.tableScan = (TableScan) new TablePlan(originalTableName, transaction).open();
            this.transaction = transaction;
            this.originalTableName = originalTableName;
        }

        @Override
        public Constant getVal(String fieldName) {
            return tableScan.getVal(fieldName);
        }

        @Override
        public void beforeFirst() {
            this.isBeforeFirst = true;
            this.tableScan.beforeFirst();
        }

        @Override
        public boolean next() {
            if (isBeforeFirst) {
                isBeforeFirst = false;
                moveToRecordIdFromQueue();
                return true;
            }
            priorityQueue.poll();
            moveToRecordIdFromQueue();
            return priorityQueue.size() > 0;
        }

        @Override
        public void close() {
            this.tableScan.close();
        }

        @Override
        public boolean hasField(String fieldName) {
            return priorityQueue.peek().containsKey(fieldName);
        }

        private void moveToRecordIdFromQueue() {
            long blockNumber = (Long) priorityQueue.peek().getVal(IvfflatIndex.SCHEMA_RID_BLOCK).asJavaVal();
            int recordId = (Integer) priorityQueue.peek().getVal(IvfflatIndex.SCHEMA_RID_ID).asJavaVal();
            RecordId recordIdObj = new RecordId(new BlockId(originalTableName + ".tbl", blockNumber), recordId);
            tableScan.moveToRecordId(recordIdObj);
        }
    }
}
