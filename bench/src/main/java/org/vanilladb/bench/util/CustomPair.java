package org.vanilladb.bench.util;


public class CustomPair<U, V> {
  private final U first;
  private final V second;

  public CustomPair(U first, V second) {
    this.first = first;
    this.second = second;
  }

  public U getFirst() {
    return first;
  }

  public V getSecond() {
    return second;
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CustomPair<?, ?> pair = (CustomPair<?, ?>) obj;
    if (first != null ? !first.equals(pair.first) : pair.first != null) {
      return false;
    }
    return second != null ? second.equals(pair.second) : pair.second == null;
  }

  @Override
  public int hashCode() {
    int result = first != null ? first.hashCode() : 0;
    result = 31 * result + (second != null ? second.hashCode() : 0);
    return result;
  }
}


