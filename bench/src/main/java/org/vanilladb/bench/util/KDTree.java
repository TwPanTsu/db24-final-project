package org.vanilladb.bench.util;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.sql.distfn.EuclideanFn;

public class KDTree {
  private final int dimensions;
  private KDNode root;

  private static class KDNode {
    private final VectorConstant x;
    private final int idx;
    private KDNode left, right;

    public KDNode(VectorConstant x, int idx) {
      this.x = x;
      this.idx = idx;
    }
  }

  public KDTree(int dimensions) {
    this.dimensions = dimensions;
  }

  public void insert(VectorConstant x, int idx) {
    root = insert(root, x, idx, 0);
  }

  private KDNode insert(KDNode node, VectorConstant x, int idx, int depth) {
    if (node == null)
      return new KDNode(x, idx);

    int axis = depth % dimensions;
    if (x.get(axis) < node.x.get(axis))
      node.left = insert(node.left, x, idx, depth + 1);
    else
      node.right = insert(node.right, x, idx, depth + 1);

    return node;
  }

  public int findNearestNeighbor(VectorConstant x) {
    return findNearestNeighbor(root, x, root.idx, 0).idx;
  }

  private KDNode findNearestNeighbor(KDNode node, VectorConstant x, int best, int depth) {
    if (node == null)
      return new KDNode(x, best);

    int axis = depth % dimensions;
    KDNode next_best = null;
    KDNode other = null;

    if (x.get(axis) < node.x.get(axis)) {
      next_best = findNearestNeighbor(node.left, x, node.idx, depth + 1);
      other = node.right;
    } else {
      next_best = findNearestNeighbor(node.right, x, node.idx, depth + 1);
      other = node.left;
    }

    if (distance(node.x, x) < distance(next_best.x, x))
      next_best = node;

    if (other != null) {
      if (distance(next_best.x, x) > Math.abs(x.get(axis) - node.x.get(axis)))
        other = findNearestNeighbor(other, x, next_best.idx, depth + 1);

      if (distance(other.x, x) < distance(next_best.x, x))
        next_best = other;
    }

    return next_best;
  }

  private double distance(VectorConstant x, VectorConstant y) {
    DistanceFn distFn = new EuclideanFn("i_emb");
    distFn.setQueryVector(x);
    return distFn.distance(new VectorConstant(y));
  }
}
