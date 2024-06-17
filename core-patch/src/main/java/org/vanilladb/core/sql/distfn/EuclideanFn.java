package org.vanilladb.core.sql.distfn;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.vanilladb.core.sql.VectorConstant;

public class EuclideanFn extends DistanceFn {
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        FloatVector sumOfSquares = FloatVector.zero(SPECIES);
        int upperLimit = SPECIES.loopBound(this.query.dimension());

        float[] queryValues = this.query.asJavaVal();
        float[] vectorValues = vec.asJavaVal();

        for (int i = 0; i < upperLimit; i += SPECIES.length()) {
            FloatVector queryVector = FloatVector.fromArray(SPECIES, queryValues, i);
            FloatVector inputVector = FloatVector.fromArray(SPECIES, vectorValues, i);
            FloatVector difference = queryVector.sub(inputVector);
            FloatVector squaredDifference = difference.mul(difference);
            sumOfSquares = squaredDifference.add(sumOfSquares);
        }

        float distance = sumOfSquares.reduceLanes(VectorOperators.ADD);

        // Process remaining elements
        for (int i = upperLimit; i < this.query.dimension(); i++) {
            float difference = this.query.get(i) - vec.get(i);
            distance += difference * difference;
        }

        return Math.sqrt(distance);
    }
}