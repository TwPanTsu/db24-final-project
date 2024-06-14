package org.vanilladb.core.storage.index.ivfflat;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.storage.record.RecordId;

public class Cluster {



    private int id;
    private VectorConstant centroid;
    private List<VectorPair<VectorConstant, RecordId>> points;

    public Cluster(int id, VectorConstant centroid) {
        this.id = id;
        this.centroid = centroid;
        this.points = new ArrayList<>();
    }

    public int getId() {
        return id;
    }

    public VectorConstant getCentroid() {
        return centroid;
    }

    public void setCentroid(VectorConstant centroid) {
        this.centroid = centroid;
    }

    public List<VectorPair<VectorConstant, RecordId>> getPoints() {
        return points;
    }

    public void addPoint(VectorPair<VectorConstant, RecordId> point) {
        points.add(point);
    }

    public void clearPoints() {
        points.clear();
    }

    public VectorConstant calculateCentroid() {
        var new_arr = new float[centroid.dimension()];
        for (VectorPair<VectorConstant, RecordId> point : points) {
            for (int i = 0; i < centroid.dimension(); i++) {
                new_arr[i] += point.VectorConst.get(i);
            }
        }
        if (points.isEmpty()) {
            return null;
        } else {
            int numPoints = points.size();
            for (int i = 0; i < centroid.dimension(); i++) {
                new_arr[i] /= numPoints;
            }
            return new VectorConstant(new_arr);
        }
    }
}