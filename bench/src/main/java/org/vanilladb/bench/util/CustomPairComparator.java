package org.vanilladb.bench.util;

import java.util.Comparator;

public  class CustomPairComparator implements Comparator<CustomPair<Double, Integer>> {
  public int compare(CustomPair<Double, Integer> c1, CustomPair<Double, Integer> c2) {
    if (c1.getFirst() < c2.getFirst())
      return 1;
    else if (c1.getFirst() > c2.getFirst())
      return -1;
    return 0;
  }
}
