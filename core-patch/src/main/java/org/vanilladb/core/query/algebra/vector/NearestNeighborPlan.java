package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.index.IndexSelectPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class NearestNeighborPlan implements Plan {
    private Plan child;
    private DistanceFn distFn;
    private Transaction tx;

    public NearestNeighborPlan(Plan p, DistanceFn distFn, Transaction tx) {
        if (!(p instanceof SelectPlan) && ! (p instanceof IndexSelectPlan))
            throw new IllegalArgumentException();
        this.child = p;
        this.tx = tx;
        this.distFn = distFn;
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        return new NearestNeighborScan(s, distFn, tx);
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
}       
