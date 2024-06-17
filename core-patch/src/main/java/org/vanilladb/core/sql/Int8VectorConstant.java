package org.vanilladb.core.sql;

import static java.sql.Types.VARCHAR;

import java.io.Serializable;

// import org.vanilladb.core.util.ByteHelper;

import java.util.*;

/**
 * Vector constant stores multiple int8 values as a constant
 * This would enable vector processing in VanillaCore
 */
public class Int8VectorConstant extends Constant implements Serializable {
    private byte[] vec;
    private Type type;

    public static Int8VectorConstant zeros(int dimension) {
        byte[] vec = new byte[dimension];
        Arrays.fill(vec, (byte) 0);
        return new Int8VectorConstant(vec);
    }

    public static Int8VectorConstant random(int dimension) {
        byte[] vec = new byte[dimension];
        Random random = new Random();
        random.nextBytes(vec);
        return new Int8VectorConstant(vec);
    }

    /**
     * Return a vector constant with random values
     * @param length size of the vector
     */
    public Int8VectorConstant(int length) {
        type = new Int8VectorType(length);
        Random random = new Random();
        vec = new byte[length];
        byte[] temp = new byte[length];
        random.nextBytes(temp);
        for (int i = 0; i < length; i++) {
            vec[i] = mapToByte(temp[i] & 0xFF);  // 確保無符號
        }
    }
    
    public Int8VectorConstant(byte[] vector) {
        type = new Int8VectorType(vector.length);
        vec = vector;
        /*
        vec = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            vec[i] = vector[i];
        }
        */
    }
    public Int8VectorConstant(float[] vector) {
        type = new Int8VectorType(vector.length);

        vec = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            vec[i] = mapToByte(Math.round(vector[i]));
        }
    
    }
    /* 
    public Int8VectorConstant(List<Byte> vector) {
        int length = vector.size();
        type = new Int8VectorType(length);
        vec = new byte[length];
        for (int i = 0; i < length; i++) {
            vec[i] = mapToByte(vector.get(i) & 0xFF);  // 確保無符號
        }
    }
    */
    public Int8VectorConstant(Int8VectorConstant v) {
        vec = new byte[v.dimension()];
        for (int i = 0; i < v.dimension(); i++) {
            vec[i] = v.get(i);  // 直接複製，因為已經是 byte 類型
        }
        type = new Int8VectorType(v.dimension());
    }
    
    public Int8VectorConstant(String vectorString) {
        String[] split = vectorString.split(" ");
        type = new Int8VectorType(split.length);
        vec = new byte[split.length];
        for (int i = 0; i < split.length; i++) {
            vec[i] = mapToByte(Math.round(Float.parseFloat(split[i])));  // 轉換為整數後映射
        }
    }
    public Int8VectorConstant(List<Float> vector) {
        int length = vector.size();
        type = new Int8VectorType(length);
        vec = new byte[length];
        for (int i = 0; i < length; i++) {
            vec[i] = mapToByte(Math.round(vector.get(i)));  // 確保無符號
        }
    }
    
    /**
     * Reconstruct a vector constant from bytes
     * @param bytes bytes to reconstruct
     */
    public Int8VectorConstant(byte[] bytes, int offset, int length) {
        type = new Int8VectorType(length);
        vec = new byte[length];
        for (int i = 0; i < length; i++) {
            vec[i] = mapToByte(bytes[offset + i] & 0xFF);  // 確保無符號
        }
    }
    
    /**
     * Map the input value to the range (-128, 127)
     * @param value input value
     * @return mapped byte value
     */
    private byte mapToByte(int value) {
        if (value < 0) {
            return (byte) -128;
        } else if (value > 255) {
            return (byte) 127;
        } else {
            return (byte) (value - 128);
        }
    }
    
    public static int[] mapToInt(byte[] byteArray) {
        int[] intArray = new int[byteArray.length];
        for (int i = 0; i < byteArray.length; i++) {
            intArray[i] = mapByteToInt(byteArray[i]);
        }
        return intArray;
    }
    
    /**
     * Map the byte value back to the original int value
     * @param value byte value
     * @return original int value
     */
    private static int mapByteToInt(byte value) {
        if (value == -128) {
            return 0;
        } else if (value == 127) {
            return 255;
        } else {
            return value + 128;
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
        return Byte.BYTES * vec.length;
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

    /*
     * To prevent overflow, directly cast to VectorContstant
     */
    @Override
    public Constant add(Constant c) {
        if (!(c instanceof Int8VectorConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support addition with other constants");
        }

        assert dimension() == ((Int8VectorConstant) c).dimension();

        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) + ((Int8VectorConstant) c).get(i);
        }
        return new VectorConstant(res);
    }

    /*
     * To prevent overflow, directly cast to VectorContstant
     */
    @Override
    public Constant sub(Constant c) {
        if (!(c instanceof Int8VectorConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support subtraction with other constants");
        }

        assert dimension() == ((Int8VectorConstant) c).dimension();

        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) - ((Int8VectorConstant) c).get(i);
        }
        return new VectorConstant(res);
    }

    /*
     * To prevent overflow, directly cast to VectorContstant
     */
    @Override
    public Constant mul(Constant c) {
        if (!(c instanceof Int8VectorConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support multiplication with other constants");
        }

        assert dimension() == ((Int8VectorConstant) c).dimension();

        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) * ((Int8VectorConstant) c).get(i);
        }
        return new VectorConstant(res);
    }

    /*
     * To prevent overflow, directly cast to VectorContstant
     */
    @Override
    public Constant div(Constant c) {
        if (!(c instanceof IntegerConstant)) {
            throw new UnsupportedOperationException("Vector doesn't support division with other constants");
        }
        float[] res = new float[dimension()];
        for (int i = 0; i < dimension(); i++) {
            res[i] = this.get(i) / (Integer) c.asJavaVal();
        }
        return new VectorConstant(res);
    }

    @Override
    public int compareTo(Constant c) {
        throw new IllegalArgumentException("Int8VectorConstant does not support comparison");
    }

    public boolean equals(Int8VectorConstant o) {
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
        return Arrays.toString(mapToInt(vec));
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
