package org.vanilladb.core.sql;

import java.sql.Types;

public class ByteVectorType extends Type {
    private int size;

    ByteVectorType(int size) {
        this.size = size;
    }

    @Override
    public int getSqlType() {
        return Types.BINARY;
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
        return size;
    }

    @Override
    public Constant maxValue() {
        throw new UnsupportedOperationException("ByteVectorType does not support maxValue()");
    }

    @Override
    public Constant minValue() {
        throw new UnsupportedOperationException("ByteVectorType does not support minValue()");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || !(obj instanceof ByteVectorType))
            return false;
        ByteVectorType t = (ByteVectorType) obj;
        return getSqlType() == t.getSqlType()
                && getArgument() == t.getArgument();
    }
}
