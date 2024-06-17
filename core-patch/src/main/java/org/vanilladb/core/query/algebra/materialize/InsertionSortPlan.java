package org.vanilladb.core.query.algebra.materialize;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A {@link SortPlan} with a limit known to be small.
 */
public class InsertionSortPlan extends SortPlan {
	private int limit;

	public InsertionSortPlan(Plan p, DistanceFn distFn, int limit, Transaction tx) {
		super(p, distFn, tx);
		this.limit = limit;
	}

	@Override
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = insertionSort(src);
		/*
		 * If the input source scan has no record, the temp table list will
		 * result in size 0. Need to check the size of "runs" here.
		 */
		if (runs.size() == 0)
			return src;
		src.close();
		return new SortScan(runs, comp, toBeFreed);
	}

	@Override
	public long recordsOutput() {
		return limit;
	}

	private List<TempTable> insertionSort(Scan src) {
		// List<TempRecordPage> toBeFreed = new ArrayList<TempRecordPage>();

		List<TempTable> temps = new ArrayList<TempTable>(1);

		src.beforeFirst();
		// if src is empty, return nothing directly
		if (!src.next())
			return temps;

		TempTable currenttemp = new TempTable(schema, tx);
		temps.add(currenttemp);
		toBeFreed.add(currenttemp);
		UpdateScan currentscan = currenttemp.open();

		int recs = 0;
		RecordId[] rids = new RecordId[limit];

		do {
			int i = recs;
			if (recs < limit) {
				currentscan.insert();
				rids[recs] = currentscan.getRecordId();
				// TODO: tx.bufferMgr().pin(rids[recs].block())?
				recs++;
			}

			while (i > 0) {
				currentscan.moveToRecordId(rids[i - 1]);
				if (comp.compare(src, currentscan) >= 0)
					break;
				if (i < recs) {
					for (String fldname : schema.fields()) {
						Constant val = currentscan.getVal(fldname);
						currentscan.moveToRecordId(rids[i]);
						currentscan.setVal(fldname, val);
						currentscan.moveToRecordId(rids[i - 1]);
					}
				}
				i--;
			}

			if (i < recs) {
				currentscan.moveToRecordId(rids[i]);
				for (String fldname : schema.fields())
					currentscan.setVal(fldname, src.getVal(fldname));
			}
		} while (src.next());

		return temps;
	}

	@Override
	public String toString() {
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		sb.append("InsertionSortPlan (#blks=" + blocksAccessed() + ", #recs="
				+ recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		;
		return sb.toString();
	}
}
