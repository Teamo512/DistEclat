package distEclat;

import static java.lang.Integer.parseInt;
//import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;
import static distEclat.DistEclatDriver.OSingletonsOrder;
import static distEclat.DistEclatDriver.OSingletonsTids;
import static distEclat.Utils.readSingletonsOrder;
import static distEclat.Utils.readTidLists;


import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class implements the Mapper for the second MapReduce cycle for Dist-Eclat. It receives a list of singletons for
 * which it has to create X-FIs seeds by growing the lattice tree downwards. The ordering is retrieved through the
 * distributed cache.
 */
public class PrefixComputerMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
  
  public static final String Fis = "fis";
  public static final String Prefix = "prefix";
  public static final String Singleton = "singleton";
  public static final String Delimiter = "$";
  
  private List<Item> singletons;
  private Map<Integer,Integer> orderMap;
  private long minSup;
  private int prefixLength;
  
  @Override
  public void setup(Context context) throws IOException {
    try {
      Configuration conf = context.getConfiguration();
      
      minSup = conf.getLong("minsup", -1);
      prefixLength = conf.getInt("prefix_length", 1);
      
      //Path[] localCacheFiles = getLocalCacheFiles(conf);
      URI[] paths = context.getCacheFiles();
      //URI[] paths = Job.getInstance(conf).getCacheFiles();
      
      for (URI path : paths) {
        String pathString = path.toString();
        if (pathString.contains(OSingletonsTids)) {
          //System.out.println("[PrefixComputerMapper]: Reading singletons");
          singletons = readTidLists(conf, new Path(path.getPath()));
        } else if (pathString.contains(OSingletonsOrder)) {
          //System.out.println("[PrefixComputerMapper]: Reading singleton orders");
          orderMap = readSingletonsOrder(new Path(path.getPath()));
        }
      }
      
      sortSingletons();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");
    String items = split[1];
    
    // if the prefix length is 1, just report the singletons, otherwise use
    // Eclat to find X-FIs seeds
    EclatMiner miner = new EclatMiner();
    SetReporter reporter = new PrefixItemTIDsReporter(context, prefixLength, singletons, orderMap);
    
    miner.setSetReporter(reporter);
    miner.setMaxSize(prefixLength);
    miner.setMinSize(prefixLength);
    
    for (String itemStr : items.split(" ")) {
      final Item item = singletons.get(orderMap.get(Integer.valueOf(itemStr)));
      assert (item.id == parseInt(itemStr));
      miner.mineRecByPruning(item, singletons, minSup);
    }
  }
  
  /**
   * Sorts the singletons using the orderings retrieved from file
   */
  private void sortSingletons() {
    Collections.sort(singletons, new Comparator<Item>() {
      @Override
      public int compare(Item o1, Item o2) {
        Integer o1Rank = orderMap.get(o1.id);
        Integer o2Rank = orderMap.get(o2.id);
        return o1Rank.compareTo(o2Rank);
      }
    });
  }
}
