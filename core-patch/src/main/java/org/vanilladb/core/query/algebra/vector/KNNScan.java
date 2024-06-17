package org.vanilladb.core.query.algebra.vector;

import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Constant;

public class KNNScan implements Scan{
    List<Constant> id_list;
    int cur;

    public KNNScan(List<Constant> list) {
        id_list = list;
        cur = -1;
    }

    @Override
    public void beforeFirst() {
        cur = -1;
    }

    @Override
    public boolean next() {
        cur++;
        return !(cur == id_list.size());
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasField(String fldName) {
        return fldName == "i_id";
    }

    @Override
    public Constant getVal(String fldName) {
        return id_list.get(cur);
    }
}
