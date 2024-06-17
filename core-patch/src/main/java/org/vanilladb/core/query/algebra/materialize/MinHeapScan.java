package org.vanilladb.core.query.algebra.materialize;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;

public class MinHeapScan implements Scan {
    private Scan src;
    private PriorityQueue<Neighbor> heap;
    private DistanceFn embfld;
    private int k;
    private Neighbor currentNeighbor;
    private List<String> fieldnames;

    public MinHeapScan(Scan src, int k, DistanceFn embfld, List<String> fieldnames) {
        this.src = src;
        this.k = k;
        this.embfld = embfld;
        this.fieldnames = fieldnames;
        this.heap = new PriorityQueue<>(k, (a, b) -> Double.compare(b.distance, a.distance));
        populateHeap();
    }

    private void populateHeap() {
        src.beforeFirst();
        while (src.next()) {
            double distance = embfld.distance((VectorConstant)src.getVal(embfld.fieldName()));
            HashMap<String, Constant> fldVals = new HashMap<>();
            for (String fldName : fieldnames) {
                fldVals.put(fldName, src.getVal(fldName));
            }
            if (heap.size() < k) {
                heap.offer(new Neighbor(distance, fldVals));
            } else if (distance < heap.peek().distance) {
                heap.poll();
                heap.offer(new Neighbor(distance, fldVals));
            }
        }
        src.close();
        if (heap.size() != k) {
          throw new IllegalStateException("Heap is not correctly populated with k elements");
      }
    }

    @Override
    public void beforeFirst() {
        // Heap is already populated, no need to reinitialize
    }

    @Override
    public boolean next() {
        if (heap.isEmpty()) {
            return false;
        }
        currentNeighbor = heap.poll();
        return true;
    }

    @Override
    public void close() {
        
    }

    @Override
    public Constant getVal(String fldName) {
        return currentNeighbor.fldVals.get(fldName);
    }

    @Override
    public boolean hasField(String fldName) {
        return fieldnames.contains(fldName);
    }

    private static class Neighbor {
        double distance;
        HashMap<String, Constant> fldVals;

        Neighbor(double distance, HashMap<String, Constant> fldVals) {
            this.distance = distance;
            this.fldVals = fldVals;
        }
    }
}
