package distEclat;

import java.util.Arrays;

public class CmdLineReporter implements SetReporter {
  
  @Override
  public void report(int[] itemset, int support) {
    System.out.println(Arrays.toString(itemset));
  }
  
  @Override
  public void close() {}
}
