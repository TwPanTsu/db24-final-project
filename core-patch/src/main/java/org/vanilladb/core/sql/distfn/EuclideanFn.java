package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
        double sum = 0;
        int i=0;
        int veclen = vec.dimension();
        for (; i < SPECIES.loopBound(veclen); i = i + SPECIES.length()) {
            var va = FloatVector.fromArray(SPECIES, vec.asJavaVal(), i); 
            var vb = FloatVector.fromArray(SPECIES, query.asJavaVal(), i);
            var vc = va.sub(vb);
            vc = vc.mul(vc);
            sum += vc.reduceLanes(VectorOperators.ADD);
        }
        if(i < veclen){
            var mask = SPECIES.indexInRange(i, veclen);
            var va = FloatVector.fromArray(SPECIES, vec.asJavaVal(), i, mask); 
            var vb = FloatVector.fromArray(SPECIES, query.asJavaVal(), i, mask);
            var vc = va.sub(vb, mask);
            vc = vc.mul(vc, mask);
            sum += vc.reduceLanes(VectorOperators.ADD, mask);
        }
        return Math.sqrt(sum);
    }
    
}
