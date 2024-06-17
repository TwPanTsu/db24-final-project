package org.vanilladb.core.query.algebra.index;

import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.index.ivf.IVFIndex;
import org.vanilladb.core.storage.record.RecordId;

public class IVFScan implements UpdateScan {
  private IVFIndex idx;
	private TableScan ts;
	private DistanceFn embField;

  public IVFScan(IVFIndex idx, DistanceFn embField, TableScan ts) {
    this.idx = idx;
    this.embField = embField;
    this.ts = ts;
  }

  @Override
	public void beforeFirst() {
		idx.beforeFirst(embField);
	}

  @Override
	public boolean next() {
		boolean ok = idx.next();
		if (ok) {
			RecordId rid = idx.getDataRecordId();
			ts.moveToRecordId(rid);
		}
		return ok;
	}

  @Override
	public void close() {
		idx.close();
		ts.close();
	}

  @Override
	public Constant getVal(String fldName) {
		return ts.getVal(fldName);
	}

  @Override
	public boolean hasField(String fldName) {
		return ts.hasField(fldName);
	}

	@Override
	public void setVal(String fldName, Constant val) {
		ts.setVal(fldName, val);
	}

	@Override
	public void delete() {
		ts.delete();
	}

	@Override
	public void insert() {
		ts.insert();
	}

	@Override
	public RecordId getRecordId() {
		return ts.getRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		ts.moveToRecordId(rid);
	}
}
