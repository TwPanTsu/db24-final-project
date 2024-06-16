package org.vanilladb.core.storage.index.ivfflat;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.record.RecordId;

public class VectorPair {
    public VectorConstant VectorConst;
    public Integer rid;
    public Long blk;

    public VectorPair(VectorConstant VectorConst, int rid,long blk) {
        this.VectorConst = VectorConst;
        this.rid = rid;
        this.blk = blk;
    }

    @Override
    public int hashCode() {
        return VectorConst.hashCode() << 16 + rid.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof VectorPair))
            return false;
        var ov = (VectorPair) other;
        return ov.VectorConst.equals(VectorConst) && ov.rid.equals(rid);
    }

    @Override
    public String toString() {
        return "< " + rid + " > || < " + VectorConst + " >";
    }
}
