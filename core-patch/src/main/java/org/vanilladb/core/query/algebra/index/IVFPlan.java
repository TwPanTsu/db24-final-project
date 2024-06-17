package org.vanilladb.core.query.algebra.index;

import java.util.Map;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.index.ivf.IVFIndex;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class IVFPlan implements Plan{
  private TablePlan tp;
	private IndexInfo ii;
	private DistanceFn embField;
	private Transaction tx;

  public IVFPlan(TablePlan tp, IndexInfo ii, DistanceFn embField, Transaction tx) {
    this.tp = tp;
    this.ii = ii;
    this.embField = embField;
    this.tx = tx;
  }

  @Override
	public Scan open() {
		// throws an exception if p is not a tableplan.
		TableScan ts = (TableScan) tp.open();
		Index idx = ii.open(tx);
		return new IVFScan( (IVFIndex) idx, embField, ts);
	}

  @Override
	public long blocksAccessed() {
		return Index.searchCost(ii.indexType(), new SearchKeyType(schema(), ii.fieldNames()),
				tp.recordsOutput(), recordsOutput()) + recordsOutput();
	}

  @Override
	public Schema schema() {
		return tp.schema();
	}

  @Override
	public Histogram histogram() {
		return tp.histogram();
	}

  @Override
	public long recordsOutput() {
		return 20;
	}
}
