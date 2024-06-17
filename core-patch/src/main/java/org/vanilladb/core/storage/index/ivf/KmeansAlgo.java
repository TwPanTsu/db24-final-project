// YAHUI: KmeansAlgo
package org.vanilladb.core.storage.index.ivf;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.EuclideanFn;
import org.vanilladb.core.storage.index.SearchKey;

import smile.math.distance.EuclideanDistance;
import java.util.Random;
import java.util.Vector;


// ASFINAL: Optimization needed
public class KmeansAlgo {

    private static List<SearchKey> trainingData;
    private static List<List<SearchKey>> resultData;
    private static List<float[]> centroidData;
    

    public KmeansAlgo(List<SearchKey> tdata) {
        trainingData = tdata;
    }

    public void KmeansTraining() {

        // init training data
        int len = trainingData.size();
        float[][] data = new float[len][IVFIndex.DIMENSION];
        int i_len = 0;
        for (SearchKey key : trainingData) {
            float[] data_point = ((VectorConstant)key.get(1)).asJavaVal();
            data[i_len] = data_point;
            i_len++;
        }

        // set the cluster
        System.out.println("Start fitting...");
        KMeans kmeans = new KMeans(IVFIndex.K, IVFIndex.random_seed);
        kmeans.fit(data);

        // set resultData (format the output data)
        int[] clusters = kmeans.predict(data);
        resultData = new ArrayList<List<SearchKey>>(IVFIndex.K);
        for (int i = 0; i < IVFIndex.K; i++) {
            resultData.add(new ArrayList<SearchKey>());
        }
        for (int i = 0; i < data.length; i++) {
            // if (i % 10000 == 0)
            //     System.out.printf("Data point %d is in cluster %d\n", i, clusters[i]);
            resultData.get(clusters[i]).add(trainingData.get(i));
        }

        // set centroidData (format the output data)
        float[][] centroids = kmeans.centroids();
        centroidData = new ArrayList<float[]>();
        for (int i = 0; i < centroids.length; i++) {
            // System.out.println("Centroid " + i + ":" + Arrays.toString(centroids[i]));
            centroidData.add(centroids[i]);
        }
    }

    

    // getter function

    public List<float[]> getCentroidList() {
        return centroidData;
    }

    public List<List<SearchKey>> getClusteringList() {
        return resultData;
    }    

    private static class KMeans {
        private int k;
        private float[][] centroids;
        private Random rand;
        private int cur_k;

        public KMeans(int k, int seed) {
            this.k = k;
            this.centroids = new float[k][IVFIndex.DIMENSION];
            this.rand = new Random(seed);
            this.cur_k = 0;
        }

        public void fit(float[][] data) {
            // Initialize centroids using KMeans++
            initializeCentroids(data);

            boolean changed;
            int[] assignments = new int[data.length];
            int iter = 0;
            do {
                System.out.println("iteration: " + iter);
                iter++;
                changed = false;
                for (int i = 0; i < data.length; i++) {
                    int newAssignment = nearestCluster(data[i]);
                    // 確認更新完centroid後，每個data point是否有更改cluster
                    if (newAssignment != assignments[i]) {
                        assignments[i] = newAssignment;
                        changed = true;
                    }
                }
                updateCentroids(data, assignments);
            } while (changed);
        }


        private void initializeCentroids(float[][] data) {
            // KMeans++ initialization
            centroids[0] = data[rand.nextInt(data.length)].clone();
            System.out.println("init centroid: 0");
            for (int i = 1; i < k; i++) {
                System.out.println("init centroid: " + i);
                float[] distances = new float[data.length];
                float sum = 0;
                for (int j = 0; j < data.length; j++) {
                    distances[j] = nearestClusterDistance(data[j]);
                    sum += distances[j];
                }
                float r = rand.nextFloat() * sum;
                sum = 0;
                for (int j = 0; j < data.length; j++) {
                    sum += distances[j];
                    if (sum >= r) {
                        centroids[i] = data[j].clone();
                        break;
                    }
                }
                cur_k++;
            }
        }

        // 與最近cluster centroid的距離
        private float nearestClusterDistance(float[] point) {
            float minDistance = Float.POSITIVE_INFINITY;
            // EuclideanDistance distance = new EuclideanDistance();
            EuclideanFn dis = new EuclideanFn("");
            dis.setQueryVector(new VectorConstant(point));
            for (int i = 0; i <= cur_k; i++) {
                int dist = (int)dis.distance(new VectorConstant(centroids[i]));
                if (dist < minDistance) {
                    minDistance = dist;
                }
            }
            return minDistance;
        }

        // 從centroid中找到最近的
        private int nearestCluster(float[] point) {
            int minCluster = 0;
            int minDistance = Integer.MAX_VALUE;
            // EuclideanDistance distance = new EuclideanDistance();
            EuclideanFn dis = new EuclideanFn("");
            dis.setQueryVector(new VectorConstant(point));
            for (int i = 0; i < k; i++) {
                int dist = (int)dis.distance(new VectorConstant(centroids[i]));
                if (dist < minDistance) {
                    minDistance = dist;
                    minCluster = i;
                }
            }
            return minCluster;
        }

        private void updateCentroids(float[][] data, int[] assignments) {
            float[][] newCentroids = new float[k][IVFIndex.DIMENSION];
            int[] counts = new int[k];

            for (int i = 0; i < data.length; i++) {
                // data i 在第幾個 cluster
                int cluster = assignments[i];
                // 把屬於同個cluster的所有點加起來
                for (int j = 0; j < IVFIndex.DIMENSION; j++) {
                    newCentroids[cluster][j] += data[i][j];
                }
                counts[cluster]++;
            }

            // 計算這k個cluster的個別新centroid
            for (int i = 0; i < k; i++) {
                if (counts[i] == 0) continue;
                for (int j = 0; j < IVFIndex.DIMENSION; j++) {
                    newCentroids[i][j] /= counts[i];
                }
            }

            centroids = newCentroids;
        }

        // 回傳每個data屬於的cluster
        public int[] predict(float[][] data) {
            int[] clusters = new int[data.length];
            for (int i = 0; i < data.length; i++) {
                clusters[i] = nearestCluster(data[i]);
            }
            return clusters;
        }

        // 回傳每個cluster的centroid point
        public float[][] centroids() {
            return centroids;
        }
    }
}

// TRY miniBatch(?)

