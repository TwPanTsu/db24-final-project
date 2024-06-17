package org.vanilladb.core.query.algebra;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class MultiTableScan implements UpdateScan {
    private List<RecordFile> rf;
    private Schema schema;
    private int rf_idx = 0;
    private int limit;

    /**
     * Creates a new table scan, and opens its corresponding record file.
     * 
     * @param ti
     *           the table's metadata
     * @param tx
     *           the calling transaction
     */
    public MultiTableScan(List<TableInfo> ti, Transaction tx, int limit) {
        this.limit = limit;
        schema = ti.get(0).schema();
        rf = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            rf.add(ti.get(i).open(tx, false));
        }

    }

    // Scan methods

    @Override
    public void beforeFirst() {
        rf.get(0).beforeFirst();
    }

    @Override
    public boolean next() {
        if (rf.get(rf_idx).next()) {
            return true;
        } else {
            if (rf_idx != (limit - 1)) {
                rf.get(rf_idx).close();
                rf_idx++;
                rf.get(rf_idx).beforeFirst();
                return rf.get(rf_idx).next();
            } else {
                return rf.get(rf_idx).next();
            }
        }
    }

    @Override
    public void close() {
        rf.get(rf_idx).close();
    }

    /**
     * Returns the value of the specified field, as a Constant.
     * 
     * @see Scan#getVal(java.lang.String)
     */
    @Override
    public Constant getVal(String fldName) {
        return rf.get(rf_idx).getVal(fldName);
    }

    @Override
    public boolean hasField(String fldName) {
        return schema.hasField(fldName);
    }

    // UpdateScan methods

    /**
     * Sets the value of the specified field, as a Constant.
     * 
     * @param val
     *            the constant to be set. Will be casted to the correct type
     *            specified in the schema of the table.
     * 
     * @see UpdateScan#setVal(java.lang.String, Constant)
     */
    @Override
    public void setVal(String fldName, Constant val) {
        rf.get(rf_idx).setVal(fldName, val);
    }

    @Override
    public void delete() {
        rf.get(rf_idx).delete();
    }

    @Override
    public void insert() {
        rf.get(rf_idx).insert();
    }

    @Override
    public RecordId getRecordId() {
        return rf.get(rf_idx).currentRecordId();
    }

    @Override
    public void moveToRecordId(RecordId rid) {
        rf.get(rf_idx).moveToRecordId(rid);
    }
}
