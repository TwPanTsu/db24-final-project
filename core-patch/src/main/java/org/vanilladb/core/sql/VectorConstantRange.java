package org.vanilladb.core.sql;

public class VectorConstantRange extends ConstantRange {
    private VectorConstant v;

    public VectorConstantRange(float[] vec) {
        v = new VectorConstant(vec);
    }

    VectorConstantRange(VectorConstant v) {
        this.v = v;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean hasLowerBound() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasUpperBound() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Constant low() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Constant high() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLowInclusive() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHighInclusive() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double length() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConstantRange applyLow(Constant c, boolean inclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConstantRange applyHigh(Constant c, boolean incl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConstantRange applyConstant(Constant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public Constant asConstant() {
        return v;
    }

    @Override
    public boolean contains(Constant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lessThan(Constant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean largerThan(Constant c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOverlapping(ConstantRange r) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(ConstantRange r) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConstantRange intersect(ConstantRange r) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConstantRange union(ConstantRange r) {
        throw new UnsupportedOperationException();
    }

}
