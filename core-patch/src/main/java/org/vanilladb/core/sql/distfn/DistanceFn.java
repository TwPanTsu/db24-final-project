package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;

public abstract class DistanceFn {

    protected VectorConstant query;
    int[] int_query;
    private String fieldName;

    public DistanceFn(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setQueryVector(VectorConstant query) {
        this.query = query;
        this.int_query = query.intVec();
    }
    
    public int distance(VectorConstant vec) {
        // check vector dimension
        if (query.dimension() != vec.dimension()) {
            throw new IllegalArgumentException("Vector length does not match");
        }
        return calculateDistance(vec);
    }

    protected abstract int calculateDistance(VectorConstant vec);

    public String fieldName() {
        return fieldName;
    }
}
