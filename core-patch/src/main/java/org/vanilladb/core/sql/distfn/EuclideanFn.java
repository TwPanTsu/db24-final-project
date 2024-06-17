package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class EuclideanFn extends DistanceFn {
    static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        int i = 0;
        FloatVector sum = FloatVector.zero(SPECIES);
        for (; i < SPECIES.loopBound(vec.dimension()); i += SPECIES.length()) {
            FloatVector v = FloatVector.fromArray(SPECIES, vec.asJavaVal(), i);
            FloatVector q = FloatVector.fromArray(SPECIES, query.asJavaVal(), i);
            FloatVector diff = v.sub(q);
            sum = diff.fma(diff, sum);
        }
        double sum_d = sum.reduceLanes(VectorOperators.ADD);
    
        // deal with tail of dim % SPECIES.length()
        for (; i < vec.dimension(); i++) {
            double diff = query.get(i) - vec.get(i);
            sum_d += diff * diff;
        }
        
        return Math.sqrt(sum_d);
    }
    
}
