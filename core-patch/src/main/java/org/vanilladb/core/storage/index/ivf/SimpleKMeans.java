package org.vanilladb.core.storage.index.ivf;

import jdk.incubator.vector.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimpleKMeans {
    private final int numClusters;
    private final int maxIterations;
    public static List<float[]> centroids;

    public SimpleKMeans(int numClusters, int maxIterations) {
        this.numClusters = numClusters;
        this.maxIterations = maxIterations;
        SimpleKMeans.centroids = new ArrayList<>(numClusters);
    }

    public List<float[]> train(List<float[]> vectors) {
        // 1. random initialize centroids
        initializeCentroids(vectors);

        for (int i = 0; i < maxIterations; i++) {
            System.out.println(i);
            // 2. 
            List<List<float[]>> clusters = new ArrayList<>(numClusters);
            for (int j = 0; j < numClusters; j++) {
                clusters.add(new ArrayList<>());
            }

            // 3. assign vectors to clusters
            for (float[] vector : vectors) {
                int closestCenterIndex = getClosestCentroidIndex(vector);
                clusters.get(closestCenterIndex).add(vector);
            }

            // 4. update centroids
            for (int j = 0; j < numClusters; j++) {
                float[] newCentroid = calculateCentroid(clusters.get(j));
                centroids.set(j, newCentroid);
            }
        }

        return centroids;
    }

    private void initializeCentroids(List<float[]> vectors) {
        Random rand = new Random();
        for (int i = 0; i < numClusters; i++) {
            int randomIndex = rand.nextInt(vectors.size());
            centroids.add(vectors.get(randomIndex));
        }
    }

    public static int getClosestCentroidIndex(float[] vector) {
        int closestIndex = 0;
        double closestDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            double distance = calculateDistance(vector, centroids.get(i));
            if (distance < closestDistance) {
                closestDistance = distance;
                closestIndex = i;
            }
        }

        return closestIndex;
    }

    public static float[] calculateCentroid(List<float[]> vectors) {
        float[] centroid = new float[vectors.get(0).length];
        for (float[] vector : vectors) {
            for (int i = 0; i < vector.length; i++) {
                centroid[i] += vector[i];
            }
        }
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] /= vectors.size();
        }
        return centroid;
    }

    private static double calculateDistance(float[] vector1, float[] vector2) {
        VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
        float sum = 0;
        int dim = 128;
        // for (int i = 0; i < vector1.length; i++) {
        //     sum += (vector1[i] - vector2[i]) * (vector1[i] - vector2[i]);
        // }
        for(int i = 0; i<SPECIES.loopBound(dim); i+=SPECIES.length()){
            FloatVector va = FloatVector.fromArray(SPECIES, vector1, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, vector2, i);
            FloatVector vc = va.sub(vb);
            FloatVector vd = vc.mul(vc);
            sum += vd.reduceLanes(VectorOperators.ADD);
        }
        return Math.sqrt(sum);
    }
}