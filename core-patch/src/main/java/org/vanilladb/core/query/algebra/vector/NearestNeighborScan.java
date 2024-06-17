package org.vanilladb.core.query.algebra.vector;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.tx.Transaction;

import java.util.PriorityQueue;

public class NearestNeighborScan implements Scan {

    Scan s;
    DistanceFn distFn;
    Transaction tx;
    private static final String FIELD = "i_id";
    Dist current;
    PriorityQueue<Dist> priorityQueue;
    boolean isBeforeFirsted = false;

    public NearestNeighborScan(Scan s, DistanceFn distFn, Transaction tx) {
        this.s = s;
        this.tx = tx;
        this.distFn = distFn;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
        priorityQueue = new PriorityQueue<>((pair1, pair2) -> Double.compare(pair1.dist, pair2.dist));

        String fld = distFn.fieldName();
        while (s.next()) {
            double dist = distFn.distance((VectorConstant) s.getVal(fld));
            priorityQueue.add(new Dist(dist, s.getVal(FIELD)));
        }
        isBeforeFirsted = true;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException();
        if (!priorityQueue.isEmpty()) {
            current = priorityQueue.poll();
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        s.close();
    }

    @Override
    public boolean hasField(String fldName) {
        return fldName.equals(FIELD);
    }

    @Override
    public Constant getVal(String fldName) {
        if (fldName.equals(FIELD)) {
            return current.id;
        }
        return null;
    }

    private static class Dist {
        double dist;
        Constant id;
        Dist(double dist, Constant id) {
            this.dist = dist;
            this.id = id;
        }
    }
}
