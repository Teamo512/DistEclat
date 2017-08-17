package distEclat;

import static java.lang.Integer.parseInt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
//import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Utils {
  
  public static List<Item> readTidLists(Configuration conf, Path path) throws IOException, URISyntaxException {
    //SequenceFile.Reader r = new SequenceFile.Reader(FileSystem.get(new URI("file:///"), conf), path, conf);
    SequenceFile.Reader r = new SequenceFile.Reader(conf, Reader.file(path));
    
    List<Item> items = new ArrayList<Item>();
    
    Text key = new Text();
    IntArrayWritable value = new IntArrayWritable();
    
    while (r.next(key, value)) {
      Writable[] tidListsW = value.get();
      
      int[] tids = new int[tidListsW.length];
      
      for (int i = 0; i < tidListsW.length; i++) {
        tids[i] = ((IntWritable) tidListsW[i]).get();
      }
      
      items.add(new Item(parseInt(key.toString()), tids.length, tids));
    }
    r.close();
    
    return items;
  }
  
  public static Map<Integer,Integer> readSingletonsOrder(Path path) throws IOException {
	  //System.out.println(path.toString());
	  //System.out.println(path.getName());
	  //System.out.println(path.getName().toString());
    BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
    
    
    String order = reader.readLine().trim();
    //System.out.println(order);
    reader.close();
    
    Map<Integer,Integer> orderMap = new HashMap<Integer,Integer>();
    String[] split = order.split(" ");
    int ix = 0;
    for (String item : split) {
      orderMap.put(Integer.valueOf(item), ix++);
    }
    return orderMap;
  }
}
