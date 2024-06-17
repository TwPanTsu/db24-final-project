package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.Int8VectorConstant;
import org.vanilladb.core.sql.VectorConstant;

public class IntEuclideanFn extends IntDistanceFn {

    public IntEuclideanFn(String fld) {
        super(fld);
    }

    // SIMD method: We don't use SIMD here! Since it is int 8, we need to iterate
    // through the vector first
    // before we can do SIMD, this cause a performance loss.

    // @Override
    // protected int calculateDistance(Int8VectorConstant vec) {
    // int[] queryArray = query.stream().mapToInt(Integer::intValue).toArray();
    // int[] vecArray = vec.stream().mapToInt(Integer::intValue).toArray();

    // int length = vec.dimension();
    // int sum = 0;
    // int i = 0;

    // // SIMD calculation
    // for (; i < SPECIES.loopBound(length); i += SPECIES.length()) {
    // var m = SPECIES.indexInRange(i, length);
    // var queryVector = IntVector.fromArray(SPECIES, queryArray, i, m);
    // var vecVector = IntVector.fromArray(SPECIES, vecArray, i, m);
    // var diff = queryVector.sub(vecVector);
    // var squaredDiff = diff.mul(diff);
    // sum += squaredDiff.reduceLanesToInt(VectorOperators.ADD);
    // }

    // // Tail loop for remaining elements
    // for (; i < length; i++) {
    // int diff = queryArray[i] - vecArray[i];
    // sum += diff * diff;
    // }

    // return sum;
    // }

    @Override
    protected int calculateDistance(Int8VectorConstant vec) {
        int sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            int diff = query.get(i) - vec.get(i);
            sum += diff * diff;
        }
        return sum;
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        double sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            double diff = query.get(i) - (float) vec.get(i);
            sum += diff * diff;
        }
        return sum;
    }
}
