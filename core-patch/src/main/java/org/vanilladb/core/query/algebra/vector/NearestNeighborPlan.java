package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.IntDistanceFn;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private Plan child;
    // 用他的 childPlan 做 Sorting (原本是SortPlan)
    public NearestNeighborPlan(Plan p, IntDistanceFn distFn, Transaction tx) {
        this.child = new TopKPlan(p, distFn, tx);
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        return new NearestNeighborScan(s);
    }

    @Override
    public long blocksAccessed() {
        return child.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return child.schema();
    }

    @Override
    public Histogram histogram() {
        return child.histogram();
    }

    @Override
    public long recordsOutput() {
        return child.recordsOutput();
    }
    @Override
	public String toString() {
		String c = child.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->NearestNeighborPlan (#blks="
				+ blocksAccessed() + ", #recs=" + recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		return sb.toString();
	}
}       
