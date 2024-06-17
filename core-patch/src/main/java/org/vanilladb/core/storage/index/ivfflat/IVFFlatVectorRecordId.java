package org.vanilladb.core.storage.index.ivfflat;

import org.vanilladb.core.sql.*;

public class IVFFlatVectorRecordId {
    VectorConstant vec;
    BigIntConstant blk;
    IntegerConstant rid;

    public IVFFlatVectorRecordId(VectorConstant vec, BigIntConstant blk, IntegerConstant rid) {
        this.vec = vec;
        this.blk = blk;
        this.rid = rid;
    }
}
