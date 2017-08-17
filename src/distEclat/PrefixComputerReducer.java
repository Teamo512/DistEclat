package distEclat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PrefixComputerReducer extends Reducer<Text,IntArrayWritable,IntArrayWritable,IntArrayWritable> {
  
  // max = 1 gb
  private static int MAX_FILE_SIZE = 1000000000;
  private static int MAX_NUMBER_OF_TIDS = (int) ((MAX_FILE_SIZE / 4) * 0.7);
  
  private static final IntArrayWritable EmptyIaw;
  
  private long minSup;
  
  private List<AtomicInteger> bucketSizes;
  
  private MultipleOutputs<IntArrayWritable,IntArrayWritable> mos;
  
  static {
    EmptyIaw = new IntArrayWritable(new IntWritable[0]);
  }
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getLong("minsup", 1);
    int numberOfMappers = conf.getInt("numMappers", 1);
    bucketSizes = new ArrayList<AtomicInteger>(numberOfMappers);
    for (int i = 0; i < numberOfMappers; i++) {
      bucketSizes.add(new AtomicInteger());
    }
    
    mos = new MultipleOutputs<IntArrayWritable,IntArrayWritable>(context);
    
  }
  
  @Override
  public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException,
      InterruptedException {
    Map<Integer,List<Integer>> map = new HashMap<Integer,List<Integer>>();
    for (IntArrayWritable iaw : values) {
      Writable[] w = iaw.get();
      int item = ((IntWritable) w[0]).get();
      
      List<Integer> tids = map.get(item);
      if (tids == null) {
        tids = new ArrayList<Integer>();
        map.put(item, tids);
      }
      for (int i = 1; i < w.length; i++) {
        int subDbTid = ((IntWritable) w[i]).get();
        tids.add(subDbTid);
      }
    }
    
    int totalTids = 0;
    for (List<Integer> tids : map.values()) {
      if (tids.size() >= minSup) {
        totalTids += tids.size();
        Collections.sort(tids);
      }
    }
    if (totalTids > 0) {
      assignToBucket(key, map, totalTids);
    }
    
  }
  
  private void assignToBucket(Text key, Map<Integer,List<Integer>> map, int totalTids) throws IOException,
      InterruptedException {
    int lowestBucket = getLowestBucket();
    if (!checkLowestBucket(lowestBucket, totalTids)) {
      bucketSizes.add(new AtomicInteger());
      lowestBucket = bucketSizes.size() - 1;
    }
    bucketSizes.get(lowestBucket).addAndGet(totalTids);
    
    String baseOutputPath = "bucket-" + lowestBucket;
    mos.write(convert(key.toString()), EmptyIaw, baseOutputPath);
    for (Entry<Integer,List<Integer>> entry : map.entrySet()) {
      if (entry.getValue().size() >= minSup) {
        IntArrayWritable owKey = convert(entry.getKey());
        IntArrayWritable owValue = convert(entry.getValue());
        mos.write(owKey, owValue, baseOutputPath);
      }
    }
    mos.write(EmptyIaw, EmptyIaw, baseOutputPath);
  }
  
  private static boolean checkLowestBucket(int lowestBucket, int totalTids) {
    return (lowestBucket + totalTids) <= MAX_NUMBER_OF_TIDS;
  }
  
  private int getLowestBucket() {
    double min = Integer.MAX_VALUE;
    int id = -1;
    int ix = 0;
    for (AtomicInteger bucketSize : bucketSizes) {
      int bs = bucketSize.get();
      if (bs < min) {
        min = bs;
        id = ix;
      }
      ix++;
    }
    return id;
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
  }
  
  private static IntArrayWritable convert(String string) {
    String[] splits = string.split(" ");
    IntWritable[] iw = new IntWritable[splits.length];
    int i = 0;
    for (String split : splits) {
      iw[i++] = new IntWritable(Integer.parseInt(split));
    }
    return new IntArrayWritable(iw);
  }
  
  private static IntArrayWritable convert(Integer key) {
    IntWritable[] iw = new IntWritable[1];
    iw[0] = new IntWritable(key);
    return new IntArrayWritable(iw);
  }
  
  private static IntArrayWritable convert(List<Integer> tidsList) {
    IntWritable[] iw = new IntWritable[tidsList.size()];
    int i = 0;
    for (int tid : tidsList) {
      iw[i++] = new IntWritable(tid);
    }
    return new IntArrayWritable(iw);
  }
}
