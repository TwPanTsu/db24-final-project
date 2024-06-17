// YAHUI: Scan
package org.vanilladb.core.query.algebra;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.ivf.IVFIndex;

public class AdvancedQueryScan implements Scan {

    public static class Pair {
        public float first;
        public int second;

        public Pair(float first, int second) {
            this.first = first;
            this.second = second;
        }
    }

    public static class MaxHeapComparator implements Comparator<Pair> {
        @Override
        public int compare(Pair p1, Pair p2) {
            return Float.compare(p2.first, p1.first);
        }
    }

    private PriorityQueue<Pair> maxHeap = new PriorityQueue<>(new MaxHeapComparator());
    private List<DistanceFn> embFields = new ArrayList<DistanceFn>();
    private List<Scan> subScans = new ArrayList<Scan>();;
    private List<Integer> dist;
    private int curCluster = 0;
    private int curReturn = 0;

    // NOTE: 寫死，根據benchmark的設定，也許可以再修改(?)
    private final int LIMITRETURN = 20;

    public AdvancedQueryScan(List<Scan> subScans, List<DistanceFn> embFields) {
        this.embFields.addAll(embFields);
        this.subScans = subScans;
        dist = new ArrayList<Integer>();
        curCluster = 0;
        curReturn = 0;
    }

    @Override
    public void beforeFirst() {
        // TODO Auto-generated method stub
        curReturn = 0;
        for (Scan s : subScans) {
            s.beforeFirst();
            while (s.next()) {
                float[] vec = (float[]) s.getVal(IVFIndex.SCHEMA_VECTOR).asJavaVal();
                Integer id = (Integer) s.getVal(IVFIndex.SCHEMA_ID).asJavaVal();
                for (DistanceFn queryVec : embFields) {
                    float dis = (float) queryVec.distance(new VectorConstant(vec));
                    maxHeap.add(new Pair(dis, id));
                    if (maxHeap.size() > 20) {
                        maxHeap.poll();
                    }
                }
            }
        }
    }

    @Override
    public Constant getVal(String fldName) {
        // System.out.println("curReturn: " + curReturn);
        // TODO Auto-generated method stub
        return new IntegerConstant(maxHeap.poll().second);
    }

    @Override
    public boolean next() {
        // TODO Auto-generated method stub
        // System.out.println("curReturn: " + curReturn);
        if (curReturn >= LIMITRETURN) {
            return false;
        }

        // // 初始化dist
        // if (curReturn == 0) {
        //     for (Scan s : subScans) {
        //         if (!s.next()) {
        //             System.err.println("Error: Not enough data to return.");
        //             return false;
        //         }
        //         float tmp_dist = 0;
                
        //         for (DistanceFn queryVec : embFields) {
        //             float[] vec = (float[]) s.getVal(IVFIndex.SCHEMA_VECTOR).asJavaVal();

        //             tmp_dist += (float) queryVec.distance(new VectorConstant(vec));
        //         }
                
        //         dist.add(tmp_dist);
        //     }
        // }
        // // 如果該cluster不足LIMIT個，則設為MAX_VALUE，避免選到
        // else if (!subScans.get(curCluster).next()) {
        //     // System.out.println("Cluster " + curCluster + " is not enough.");
        //     dist.set(curCluster, Float.MAX_VALUE);
        //     // return true;
        // }
        // // 把next過的cluster的dist重新計算
        // else {
        //     // System.out.println("Cluster " + curCluster + " is enough.");
        //     float tmp_dist = 0;
        //     for (DistanceFn queryVec : embFields) {
        //         float[] vec = (float[]) subScans.get(curCluster).getVal(IVFIndex.SCHEMA_VECTOR).asJavaVal();
        //         tmp_dist += (float) queryVec.distance(new VectorConstant(vec));
        //     }
        //     dist.set(curCluster, tmp_dist);
        // }
        curReturn++;
        return true;
    }


    @Override
    public void close() {
        // TODO Auto-generated method stub
        for (Scan s : subScans) {
            s.close();
        }
    }

    @Override
    public boolean hasField(String fldName) {
        // TODO Auto-generated method stub
        for (Scan s : subScans) {
            if (!s.hasField(fldName)) {
                return false;
            }
        }
        return true;
    }


    public void setCluster(int cluster) {
        curCluster = cluster;
    }

}
