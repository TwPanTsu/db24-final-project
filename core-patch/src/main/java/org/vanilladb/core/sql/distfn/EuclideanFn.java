package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.*;
import jdk.incubator.vector.IntVector;

public class EuclideanFn extends DistanceFn {

    final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected int calculateDistance(VectorConstant vec) {
        // System.out.println("rrrrrrrrrrrrrr\nrrrr\nr\nrrrrrrrrrrrrrrrrr");
        int[] vec_arr = vec.intVec();
        // int sum = 0;
        // for (int i=0;i<vec.dimension();i++) {
        //     int diff = vec_arr[i] - int_query[i];
        //     sum += diff * diff;
        // }
        // return sum;

        
        IntVector tmp = IntVector.zero(SPECIES);

        for (int i=0; i<SPECIES.loopBound(vec.dimension()); i+=SPECIES.length()) {
            IntVector vec_dv = IntVector.fromArray(SPECIES, vec_arr, i);
            IntVector query_dv = IntVector.fromArray(SPECIES, int_query, i);
            IntVector diff = vec_dv.sub(query_dv);
            tmp = diff.mul(diff).add(tmp);
        }

        int sum = tmp.reduceLanes(VectorOperators.ADD);
        return sum;
    }
    
}
