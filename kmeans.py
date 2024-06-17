import numpy as np
from sklearn.cluster import MiniBatchKMeans
import os
import argparse

def load_data(file_path, max_samples=90000):
    data = []
    # print(f"max samples: {max_samples}")
    with open(file_path, 'r') as file:
        for i, line in enumerate(file):
            if i >= max_samples:
                break
            values = list(map(float, line.strip().split()))
            data.append(values)
    return np.array(data)

def main(file_path, num_items, n_clusters, batch_size, output_dir):
    # 讀取資料
    data = load_data(file_path, num_items)

    # 定義 K-means 模型
    kmeans = MiniBatchKMeans(n_clusters=n_clusters, batch_size=batch_size, random_state=42)

    # 擬合模型
    kmeans.fit(data)

    # 取得聚類中心
    centroids = kmeans.cluster_centers_

    # 創建輸出資料夾
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 輸出聚類中心到檔案
    centroids_file = os.path.join(output_dir, 'centroids.txt')
    np.savetxt(centroids_file, centroids, fmt='%f')
    print(f"Clustering completed. Centroids saved to {centroids_file}")

    # 初始化聚類成員列表
    clusters = [[] for _ in range(n_clusters)]
    cluster_indices = [[] for _ in range(n_clusters)]

    # 將資料分配到各個聚類中
    labels = kmeans.labels_
    for index, label in enumerate(labels):
        clusters[label].append(data[index])
        cluster_indices[label].append(index)

    # 輸出每個聚類的所有成員及其索引到獨立文件
    for i in range(n_clusters):
        cluster_data_file = os.path.join(output_dir, f'cluster_{i}.txt')
        cluster_indices_file = os.path.join(output_dir, f'cluster_{i}_indices.txt')
        
        np.savetxt(cluster_data_file, clusters[i], fmt='%f')
        np.savetxt(cluster_indices_file, cluster_indices[i], fmt='%d')


    print(f"Cluster members and their indices saved to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='K-means clustering with MiniBatchKMeans')
    parser.add_argument('--file_path', type=str, default="sift.txt", help='Path to the input data file')
    parser.add_argument('--num_items', type=int, default=900, help='Path to the input data file')
    parser.add_argument('--n_clusters', type=int, default=30, help='Number of clusters for K-means')
    parser.add_argument('--batch_size', type=int, default=10000, help='Batch size for MiniBatchKMeans')
    parser.add_argument('--output_dir', type=str, default='clusters_output', help='Directory to save output files')
    
    args = parser.parse_args()
    print(f"file_path:{args.file_path} / num_items:{args.num_items} / n_clusters:{args.n_clusters} / batch_size:{args.batch_size} / output_dir:{args.output_dir}")
    main(args.file_path, args.num_items ,args.n_clusters, args.batch_size, args.output_dir)
