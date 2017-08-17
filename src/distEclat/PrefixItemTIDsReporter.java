package distEclat;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//@SuppressWarnings({"rawtypes", "unchecked"})
public class PrefixItemTIDsReporter implements SetReporter {
  
  private Mapper<LongWritable,Text,Text,IntArrayWritable>.Context context;
  private int prefixLength;
  private List<Item> singletons;
  private Map<Integer,Integer> orderMap;
  
  
  public PrefixItemTIDsReporter(Mapper<LongWritable,Text,Text,IntArrayWritable>.Context context, int prefixLength, List<Item> singletons, Map<Integer,Integer> orderMap) {
    this.context = context;
    this.prefixLength = prefixLength;
    this.singletons = singletons;
    this.orderMap = orderMap;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    StringBuilder sb = new StringBuilder();
    if (itemset.length < prefixLength) {
      System.out.println("Found a short fis:" + Arrays.toString(itemset));
      return;
    }
    int prefixStrLength = 0;
    int lastItem = -1;
    for (int item : itemset) {
      prefixStrLength = sb.length() - 1;
      sb.append(item).append(" ");
      lastItem = item;
    }
    sb.setLength(prefixStrLength);
    
    Text key = new Text(sb.toString());
    
    int[] tids = computeTids(itemset);
    
    IntWritable[] iw = new IntWritable[tids.length + 1];
    
    for (int i = 1; i < iw.length; i++) {
      iw[i] = new IntWritable(tids[i - 1]);
    }
    iw[0] = new IntWritable(lastItem);
    
    try {
      context.write(key, new IntArrayWritable(iw));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private int[] computeTids(int[] itemset) {
    final int[] firstTids = singletons.get(orderMap.get(itemset[0])).getTids();
    int[] tids = Arrays.copyOf(firstTids, firstTids.length);
    
    for (int i = 1; i < itemset.length; i++) {
      Item item = singletons.get(orderMap.get(itemset[i]));
      tids = Tools.intersect(tids, item.getTids());
    }
    return tids;
  }
  
  @Override
  public void close() {
    
  }
  
}
