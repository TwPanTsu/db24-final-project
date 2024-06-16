package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorMask;

public class EuclideanFn extends DistanceFn {

    public EuclideanFn(String fld) {
        super(fld);
    
    }
    static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_256;
    
    @Override
    public double calculateDistance(VectorConstant vec) {

        double sum = 0;
        float[] a = vec.asJavaVal();
        float[] b = query.asJavaVal();
        int length  = a.length;
        float[] result = new float[SPECIES.length()];
        
        for (int i = 0; i < length; i += SPECIES.length()) {

            VectorMask<Float> m = SPECIES.indexInRange(i, length);
            FloatVector va = FloatVector.fromArray(SPECIES, a, i, m);
            FloatVector vb = FloatVector.fromArray(SPECIES, b, i, m);
            FloatVector diff = va.sub(vb);
            FloatVector sq = diff.mul(diff);
            sq.intoArray(result, 0, m);
            
            for (int j = 0; j < m.length(); j++) {
                sum += result[j];
            }
        }
        // for (int i = 0; i < vec.dimension(); i++) {
        //     double diff = query.get(i) - vec.get(i);
        //     sum += diff * diff;
        // }
        return Math.sqrt(sum);
    }
    
}
