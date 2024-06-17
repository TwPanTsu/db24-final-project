package org.vanilladb.core.query.algebra.vector;

import java.util.List;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.sql.Schema;

public class KNNPlan implements Plan{
    private Plan child;
    private Transaction tx;
    private DistanceFn distfn;

    public KNNPlan(Plan p, DistanceFn distFn, Transaction txs) {
        this.distfn = distFn;
        this.child = new SortPlan(p, distFn, txs);
        tx = txs;
    }

    @Override
    public Scan open() {
        List<Constant> id_list = VanillaDb.ivfIndex.find_cluster(distfn.getQueryVector(), tx);
        return new KNNScan(id_list);
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
