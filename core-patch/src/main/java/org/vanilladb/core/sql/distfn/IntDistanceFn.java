package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.sql.VectorConstant;

public abstract class IntDistanceFn {

    protected Int8VectorConstant query;
    private String fieldName;

    public IntDistanceFn(String fieldName) {
        this.fieldName = fieldName;
    }

    public void setQueryVector(Int8VectorConstant query) {
        this.query = query;
    }
    
    public int distance(Int8VectorConstant vec) {
        // check vector dimension
        if (query.dimension() != vec.dimension()) {
            throw new IllegalArgumentException("Vector length does not match");
        }
        return calculateDistance(vec);
    }

    protected abstract int calculateDistance(Int8VectorConstant vec);
    protected abstract double calculateDistance(VectorConstant vec);

    public String fieldName() {
        return fieldName;
    }

    public Int8VectorConstant queryVector() {
        return query;
    }
}
