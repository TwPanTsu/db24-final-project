package org.vanilladb.core.storage.index.ivfflat;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.record.RecordId;

public class VectorPairComparable extends VectorPair implements Comparable<VectorPairComparable> {
    private DistanceFn distFn;
    private double distToTarget = -1;

    public VectorPairComparable(VectorConstant VectorConst, RecordId rid, DistanceFn distFn) {
        super(VectorConst, rid);
        this.distFn = distFn;
    }

    @Override
    public int compareTo(VectorPairComparable other){
        if (distToTarget == -1)
            distToTarget = distFn.distance(this.VectorConst);
        if (other.distToTarget == -1)
            other.distToTarget = distFn.distance(other.VectorConst);
        return Double.compare(distToTarget, other.distToTarget);
    }
}
