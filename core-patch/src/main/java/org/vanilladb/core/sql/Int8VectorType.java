package org.vanilladb.core.sql;

import java.sql.Types;

/**
 * The type of a vector constant.
 */
public class Int8VectorType extends Type {
    private int size;

    public Int8VectorType(int size) {
        this.size = size;
    }

    @Override
    public int getSqlType() {
        return 2000;
    }

    @Override
    public int getArgument() {
        return size;
    }

    @Override
    public boolean isFixedSize() {
        return true;
    }

    @Override
    public boolean isNumeric() {
        return false;
    }

    @Override
    public int maxSize() {
        return size; // 每個 element 都是 1 bytes，所以直接回傳size
    }

    @Override
    public Constant maxValue() {
        throw new UnsupportedOperationException("Int8VectorConstant does not support maxValue()");
    }

    @Override
    public Constant minValue() {
        throw new UnsupportedOperationException("Int8VectorConstant does not support minValue()");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || !(obj instanceof Int8VectorType))
            return false;
        Int8VectorType t = (Int8VectorType) obj;
        return getSqlType() == t.getSqlType()
                && getArgument() == t.getArgument();
    }
}
