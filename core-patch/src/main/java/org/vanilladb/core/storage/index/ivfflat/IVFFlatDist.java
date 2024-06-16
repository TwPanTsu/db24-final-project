package org.vanilladb.core.storage.index.ivfflat;

public class IVFFlatDist {
    int idx;
    double dist;
    int itemCount;

    public IVFFlatDist(int idx, double dist, int itemCount) {
        this.idx = idx;
        this.dist = dist;
        this.itemCount = itemCount;
    }

    public double getDist() {
        return dist;
    }

    public int getIdx() {
        return idx;
    }

    public int getItemCount() {
        return itemCount;
    }
}
