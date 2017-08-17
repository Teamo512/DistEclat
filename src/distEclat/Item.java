package distEclat;

import java.util.Arrays;

public class Item {
  
  public final int id;
  private final int support;
  protected final int[] tids;
  
  public Item(int id, int support, int[] tids) {
    this.id = id;
    this.support = support;
    this.tids = tids;
  }

  
  public int freq() {
    return support;
  }
  
  public int[] getTids() {
    return tids;
  }
  
  @Override
  public String toString() {
    return id + " (" + freq() + ")" + " [" + tids.length + "]";
  }


  @Override
  public int hashCode() {
    return id;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Item other = (Item) obj;
    if (id != other.id) return false;
    if (support != other.support) return false;
    if (!Arrays.equals(tids, other.tids)) return false;
    return true;
  }
  
}