package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
//SIMD
import jdk.incubator.vector.*;

public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    }

    // @Override
    // protected double calculateDistance(VectorConstant vec) {
    //     double sum = 0;
    //     for (int i = 0; i < vec.dimension(); i++) {
    //         double diff = query.get(i) - vec.get(i);
    //         sum += diff * diff;
    //     }
    //     return Math.sqrt(sum);
    // }
    
    //SIMD change to float for faster calculation
    @Override
    protected double calculateDistance(VectorConstant vector) {
        VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
        int i = 0;
        float sum = 0.0f;
        int dim = vector.dimension();
        
        for (; i < SPECIES.loopBound(dim); i += SPECIES.length()) {
            FloatVector va = FloatVector.fromArray(SPECIES, query.vec, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, vector.vec, i);
            FloatVector vc = va.sub(vb);
            FloatVector vd = vc.mul(vc);
            sum += vd.reduceLanes(VectorOperators.ADD);
        }

        /* 
        for (; i < dim; i++) {   
            double diff = query.get(i) - vector.get(i);
            sum += diff * diff;
        }*/
        return Math.sqrt(sum);
    }
    
}
