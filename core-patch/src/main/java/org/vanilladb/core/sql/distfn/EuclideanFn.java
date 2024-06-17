package org.vanilladb.core.sql.distfn;

// import java.lang.invoke.ClassSpecializer.SpeciesData;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {

    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    // @Override
    // protected double calculateDistance(VectorConstant vec) {
    // double sum = 0;
    // for (int i = 0; i < vec.dimension(); i++) {
    // double diff = query.get(i) - vec.get(i);
    // sum += diff * diff;
    // }
    // return Math.sqrt(sum);
    // }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        int len = vec.dimension();
        float[] queryArray = query.asJavaVal();
        float[] vecArray = vec.asJavaVal();

        float sum = 0.0f;
        int i = 0;

        // SIMD
        // Use SIMD for the bulk of the computation
        // If SPECIES.length() is 4 and length is 10, SPECIES.loopBound(length) will
        // return 8. This means the loop will process elements in chunks of 4 up to
        // index 8, and any remaining elements (from index 8 to 9) will be handled
        // separately.
        for (; i < SPECIES.loopBound(len); i += SPECIES.length()) {
            FloatVector vQuery = FloatVector.fromArray(SPECIES, queryArray, i);
            FloatVector vVec = FloatVector.fromArray(SPECIES, vecArray, i);
            FloatVector diff = vQuery.sub(vVec);
            FloatVector sqr = diff.mul(diff);
            // reduceLanes(): This method takes in a mathematical operation, such as ADD,
            // and combines all elements of the vector into a single value
            sum += sqr.reduceLanes(VectorOperators.ADD);
        }

        // Handle any remaining elements (if length is not a multiple of
        // SPECIES.length())
        for (; i < len; i++) {
            float diff = queryArray[i] - vecArray[i];
            sum += diff * diff;
        }
        // this will return double
        return Math.sqrt(sum);
    }

    // @Override
    // protected double calculateDistance(VectorConstant vec) {
    // int len = vec.dimension();
    // float[] queryArray = query.asJavaVal();
    // float[] vecArray = vec.asJavaVal();

    // float sum = 0.0f;
    // int i = 0;

    // for (; i < len; i += SPECIES.length()) {
    // FloatVector vQuery = FloatVector.fromArray(SPECIES, queryArray, i);
    // FloatVector vVec = FloatVector.fromArray(SPECIES, vecArray, i);
    // FloatVector diff = vQuery.sub(vVec);
    // FloatVector sqr = diff.mul(diff);
    // // reduceLanes(): This method takes in a mathematical operation, such as ADD,
    // // and combines all elements of the vector into a single value
    // sum += sqr.reduceLanes(VectorOperators.ADD);
    // }

    // // this will return double
    // return Math.sqrt(sum);
    // }

}
