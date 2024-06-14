package org.vanilladb.core.storage.index.ivfflat;

public class VectorPair<V, R> {
    public V VectorConst;
    public R rid;

    public VectorPair(V VectorConst, R rid) {
        this.VectorConst = VectorConst;
        this.rid = rid;
    }
}
