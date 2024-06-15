package org.vanilladb.core.sql;

public class VectorConstantRange extends ConstantRange {
    private VectorConstant lowerBound;
    private VectorConstant upperBound;
    private boolean isLowerInclusive;
    private boolean isUpperInclusive;

    /**
     * Constructs a VectorConstantRange with specified bounds.
     *
     * @param lowerBound the lower bound of the range (null if unbounded)
     * @param isLowerInclusive true if the lower bound is inclusive
     * @param upperBound the upper bound of the range (null if unbounded)
     * @param isUpperInclusive true if the upper bound is inclusive
     */
    public VectorConstantRange(VectorConstant lowerBound, boolean isLowerInclusive,
                               VectorConstant upperBound, boolean isUpperInclusive) {
        this.lowerBound = lowerBound;
        this.isLowerInclusive = isLowerInclusive;
        this.upperBound = upperBound;
        this.isUpperInclusive = isUpperInclusive;
    }

    /*
     * Getters
     */

    @Override
    public boolean isValid() {
        if (lowerBound != null && upperBound != null && isLowerInclusive && isUpperInclusive) {
            return lowerBound.equals(upperBound);
        }
        return false;
    }

    @Override
    public boolean hasLowerBound() {
        return lowerBound != null;
    }

    @Override
    public boolean hasUpperBound() {
        return upperBound != null;
    }

    @Override
    public Constant low() {
        if (lowerBound != null) {
            return lowerBound;
        }
        throw new IllegalStateException("Lower bound is not set.");
    }

    @Override
    public Constant high() {
        if (upperBound != null) {
            return upperBound;
        }
        throw new IllegalStateException("Upper bound is not set.");
    }

    @Override
    public boolean isLowInclusive() {
        return isLowerInclusive;
    }

    @Override
    public boolean isHighInclusive() {
        return isUpperInclusive;
    }

    @Override
    public double length() {
        throw new UnsupportedOperationException("Length operation is not supported for VectorConstantRange.");
    }

    /*
     * Constant operations
     */

    @Override
    public ConstantRange applyLow(Constant constant, boolean inclusive) {
        if (!(constant instanceof VectorConstant)) {
            throw new IllegalArgumentException("Argument must be a VectorConstant.");
        }
        if (!constant.equals(lowerBound)) {
            throw new IllegalArgumentException("Constant does not match the lower bound.");
        }
        return new VectorConstantRange(lowerBound, isLowerInclusive, upperBound, isUpperInclusive);
    }

    @Override
    public ConstantRange applyHigh(Constant constant, boolean inclusive) {
        if (!(constant instanceof VectorConstant)) {
            throw new IllegalArgumentException("Argument must be a VectorConstant.");
        }
        if (!constant.equals(upperBound)) {
            throw new IllegalArgumentException("Constant does not match the upper bound.");
        }
        return new VectorConstantRange(lowerBound, isLowerInclusive, upperBound, isUpperInclusive);
    }

    @Override
    public ConstantRange applyConstant(Constant constant) {
        if (!(constant instanceof VectorConstant)) {
            throw new IllegalArgumentException("Argument must be a VectorConstant.");
        }
        return applyLow(constant, true).applyHigh(constant, true);
    }

    @Override
    public boolean isConstant() {
        return lowerBound.equals(upperBound) && isLowerInclusive && isUpperInclusive;
    }

    @Override
    public Constant asConstant() {
        if (isConstant()) {
            return lowerBound;
        }
        throw new IllegalStateException("Range does not represent a constant value.");
    }

    @Override
    public boolean contains(Constant constant) {
        if (!(constant instanceof VectorConstant)) {
            throw new IllegalArgumentException("Argument must be a VectorConstant.");
        }
        if (!isValid()) {
            return false;
        }
		/*
		 * Note that if low and high are INF ore NEG_INF here, using
		 * c.compare(high/low) will have wrong answer.
		 * 
		 * For example, if high=INF, the result of c.compareTo(high) is the same
		 * as c.compareTo("Infinity").
		 */
        return constant.equals(lowerBound);
    }

    @Override
    public boolean lessThan(Constant constant) {
        if (lowerBound.equals(constant)) {
            return false;
        }
        throw new IllegalArgumentException("Lower bound does not match the constant.");
    }

    @Override
    public boolean largerThan(Constant constant) {
        if (lowerBound.equals(constant)) {
            return false;
        }
        throw new IllegalArgumentException("Lower bound does not match the constant.");
    }

    /*
     * Range operations
     */

    @Override
    public boolean isOverlapping(ConstantRange range) {
        if (!(range instanceof VectorConstantRange)) {
            throw new IllegalArgumentException("Argument must be a VectorConstantRange.");
        }
        if (!isValid() || !range.isValid()) {
            return false;
        }
        return range.low().equals(lowerBound);
    } 

	// TODO : check the possible of c.compareTo(INF)
    @Override
    public boolean contains(ConstantRange range) {
        if (!(range instanceof VectorConstantRange)) {
            throw new IllegalArgumentException("Argument must be a VectorConstantRange.");
        }
        if (!isValid() || !range.isValid()) {
            return false;
        }
        return range.low().equals(lowerBound);
    }

    @Override
    public ConstantRange intersect(ConstantRange range) {
        if (!(range instanceof VectorConstantRange)) {
            throw new IllegalArgumentException("Argument must be a VectorConstantRange.");
        }
        if (range.low().equals(lowerBound) && isValid() && range.isValid()) {
            return new VectorConstantRange(lowerBound, isLowerInclusive, upperBound, isUpperInclusive);
        }
        return new VectorConstantRange(null, isLowerInclusive, null, isUpperInclusive);
    }

    @Override
    public ConstantRange union(ConstantRange range) {
        if (!(range instanceof VectorConstantRange)) {
            throw new IllegalArgumentException("Argument must be a VectorConstantRange.");
        }
        if (range.low().equals(lowerBound) && isValid() && range.isValid()) {
            return new VectorConstantRange(lowerBound, isLowerInclusive, upperBound, isUpperInclusive);
        }
        return new VectorConstantRange(null, isLowerInclusive, null, isUpperInclusive);
    }
}
