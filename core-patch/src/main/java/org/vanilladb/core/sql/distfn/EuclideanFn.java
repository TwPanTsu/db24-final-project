package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    }

    /*@Override
    protected double calculateDistance(VectorConstant vec) {
        double sum = 0;
        for (int i = 0; i < vec.dimension(); i++) {
            double diff = query.get(i) - vec.get(i);
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }*/
    private static VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
    @Override
    protected double calculateDistance(VectorConstant vec) {    // Final (SIMD).
        double[] vec_d = floatToDoubleArray(vec.asJavaVal());
        double[] query_d = floatToDoubleArray(query.asJavaVal()); 
        if (vec_d.length != query_d.length) {
            throw new IllegalArgumentException("Arrays must have the same length");
        }
        double distanceSquared = 0.0;
        int i = 0;
        for(; i < SPECIES.loopBound(vec.dimension()); i += SPECIES.length()) {
            var va = DoubleVector.fromArray(SPECIES, vec_d, i);
            var vb = DoubleVector.fromArray(SPECIES, query_d, i);
            var diff = va.sub(vb);
            var square = diff.mul(diff);
            distanceSquared += square.reduceLanes(DoubleVector.ADD);
        }
        
        for (; i < vec.dimension(); i++) {
            double diff = query_d[i] - vec_d[i];
            distanceSquared += diff * diff;
        }

        return Math.sqrt(distanceSquared);
    }
    
    public static double[] floatToDoubleArray(float[] floatArray) {
        double[] doubleArray = new double[floatArray.length];
        for (int i = 0; i < floatArray.length; i++) {
            doubleArray[i] = floatArray[i];
        }
        return doubleArray;
    }
}
