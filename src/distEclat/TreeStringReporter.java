package distEclat;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TreeStringReporter implements SetReporter {
  private static final int MAX_SETS_BUFFER = 1000000;
  
  private final Context context;
  private final StringBuilder builder;
  
  private int[] prevSet;
  private int count;
  
  public TreeStringReporter(Context context) {
    this.context = context;
    builder = new StringBuilder();
    count = 0;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    
    if (prevSet == null) {
      for (int i = 0; i < itemset.length - 1; i++) {
        builder.append(itemset[i]).append(TriePrinter.SEPARATOR);
      }
    } else {
      int depth = 0;
      while (depth < itemset.length && depth < prevSet.length && itemset[depth] == prevSet[depth]) {
        depth++;
      }
      
      for (int i = prevSet.length - depth; i > 0; i--) {
        builder.append(TriePrinter.SYMBOL);
      }
      for (int i = depth; i < itemset.length - 1; i++) {
        builder.append(itemset[i]).append(TriePrinter.SEPARATOR);
      }
    }
    builder.append(itemset[itemset.length - 1]).append(TriePrinter.OPENSUP).append(support).append(TriePrinter.CLOSESUP);
    prevSet = Arrays.copyOf(itemset, itemset.length);
    count++;
    if (count % MAX_SETS_BUFFER == 0) {
      try {
        context.write(new Text("" + count), new Text(builder.toString()));
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("wrote " + count + " compressed itemsets");
      builder.setLength(0);
      count = 0;
    }
  }
  
  @Override
  public void close() {
    try {
      context.write(new Text("" + count), new Text(builder.toString()));
      System.out.println("wrote " + count + " compressed itemsets");
      builder.setLength(0);
      count = 0;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
