package org.vanilladb.core.sql;

import static java.sql.Types.VARCHAR;

import java.io.Serializable;
import java.util.*;

public class ByteVectorConstant extends Constant implements Serializable {
    private byte[] vec;
    private Type type;

    public static ByteVectorConstant zeros(int dimension) {
        byte[] vec = new byte[dimension];
        for (int i = 0; i < dimension; i++) {
            vec[i] = 0;
        }
        return new ByteVectorConstant(vec);
    }

    /**
     * Return a vector constant with random values
     * @param length size of the vector
     */
    public ByteVectorConstant(int length) {
        type = new ByteVectorType(length);
        Random random = new Random();
        vec = new byte[length];
        random.nextBytes(vec);
    }

    public ByteVectorConstant(byte[] vector) {
        type = new ByteVectorType(vector.length);
        vec = new byte[vector.length];
        
        for (int i = 0; i < vector.length; i++) {
            vec[i] = vector[i];
        }
    }

    public ByteVectorConstant(List<Byte> vector) {
        int length = vector.size();
        
        type = new ByteVectorType(length);
        vec = new byte[length];
        
        for (int i = 0; i < length; i++) {
            vec[i] = vector.get(i);
        }
    }

    public ByteVectorConstant(ByteVectorConstant v) {
        vec = new byte[v.dimension()];
        int i = 0;
        for (byte e : v.asJavaVal()) {
            vec[i++] = e;
        }
        type = new ByteVectorType(v.dimension());
    }

    public ByteVectorConstant(VectorConstant v) {
        vec = new byte[v.dimension()];
        int i = 0;
        for (float e : v.asJavaVal()) {
            // Direct SQ8 quantization is here
            // -128 because "unsigned byte" is awkward in Java
            vec[i++] = (byte)(Math.min(Math.max(Math.round(e), 0), 255) - 128);
        }
        type = new ByteVectorType(v.dimension());
    }

    public ByteVectorConstant(String vectorString) {
        String[] split = vectorString.split(" ");

        type = new ByteVectorType(split.length);
        vec = new byte[split.length];

        for (int i = 0; i < split.length; i++) {
            vec[i] = Byte.valueOf(split[i]);
        }
    }

    /**
     * Return the type of the constant
     */
    @Override
    public Type getType() {
        return type;
    }

    /**
     * Return the value of the constant
     */
    @Override
    public byte[] asJavaVal() {
        return vec;
    }

    /**
     * Return a copy of the vector
     * @return
     */
    public byte[] copy() {
        return Arrays.copyOf(vec, vec.length);
    }


    /** 
     * Return the vector as bytes
    */
    @Override
    public byte[] asBytes() {
        return Arrays.copyOf(vec, vec.length);
    }

    /**
     * Return the size of the vector in bytes
     */
    @Override
    public int size() {
        return vec.length;
    }

    /**
     * Return the size of the vector
     * @return size of the vector
     */
    public int dimension() {
        return vec.length;
    }

    @Override
    public Constant castTo(Type type) {
        if (getType().equals(type))
            return this;
        switch (type.getSqlType()) {
            case VARCHAR:
                return new VarcharConstant(toString(), type);
            }
        throw new IllegalArgumentException("Cannot cast vector to " + type);
    }

    public byte get(int idx) {
        return vec[idx];
    }

    @Override
    public Constant add(Constant c) {
        throw new UnsupportedOperationException("ByteVectorConstant doesn't support addition");
    }

    @Override
    public Constant sub(Constant c) {
        throw new UnsupportedOperationException("ByteVectorConstant doesn't support subtraction");
    }

    @Override
    public Constant mul(Constant c) {
        throw new UnsupportedOperationException("ByteVectorConstant doesn't support multiplication");
    }

    @Override
    public Constant div(Constant c) {
        throw new UnsupportedOperationException("ByteVectorConstant doesn't support division");
    }

    @Override
    public int compareTo(Constant c) {
        throw new IllegalArgumentException("ByteVectorConstant does not support comparison");
    }

    public boolean equals(ByteVectorConstant o) {
        if (o.size() != this.size())
            return false;

        for (int i = 0; i < dimension(); i++) {
            if (vec[i] != o.get(i))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(vec);
    }

    public int[] hashCode(int bands, int buckets) {
        assert dimension() % bands == 0;

        int chunkSize = dimension() / bands;

        int[] hashCodes = new int[bands];
        for (int i = 0; i < bands; i++) {
            int hashCode = (Arrays.hashCode(Arrays.copyOfRange(vec, i * chunkSize, (i + 1) * chunkSize))) % buckets;
            if (hashCode < 0)
                hashCode += buckets;
            hashCodes[i] = hashCode;
        }
        return hashCodes;
    }
}
