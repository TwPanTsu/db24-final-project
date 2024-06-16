package org.vanilladb.core.query.algebra.materialize;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

import java.util.ArrayList;
import java.util.List;

public class MinHeapPlan implements Plan {
    private Plan p;
    private int k;
    private DistanceFn distFn;
    private List<String> fieldnames = new ArrayList<>();

    public MinHeapPlan(Plan p, int k, DistanceFn distFn) {
        this.p = p;
        this.k = k;
        this.distFn = distFn;
        for (String fldName : p.schema().fields()) {
            fieldnames.add(fldName);
        }
    }

    @Override
    public Scan open() {
        return new MinHeapScan(p.open(), k, distFn, fieldnames);
    }

    @Override
    public long blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return p.schema();
    }

    @Override
    public Histogram histogram() {
        return p.histogram();
    }

    @Override
    public long recordsOutput() {
        return k;
    }

    @Override
    public String toString() {
        String c = p.toString();
        String[] cs = c.split("\n");
        StringBuilder sb = new StringBuilder();
        sb.append("->");
        sb.append("MinHeapPlan (top " + k + " neighbors)\n");
        for (String child : cs) {
            sb.append("\t").append(child).append("\n");
        }
        return sb.toString();
    }
}
