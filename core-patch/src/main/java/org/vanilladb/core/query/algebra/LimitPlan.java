package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class LimitPlan implements Plan {

    private Plan child;
    private int limit;

    public LimitPlan(Plan p, int limit) {
        this.child = p;
        this.limit = limit;
    }

    @Override
    public Scan open() {
        Scan s = child.open();
        return new LimitScan(s, limit);
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
        return limit;
    }

    @Override
    public String toString() {
        String c = child.toString();
        String[] cs = c.split("\n");
        StringBuilder sb = new StringBuilder();
        sb.append("->LimitPlan (#blks="
                + blocksAccessed() + ", #recs=" + recordsOutput() + ")\n");
        for (String child : cs)
            sb.append("\t").append(child).append("\n");
        return sb.toString();
    }
}
