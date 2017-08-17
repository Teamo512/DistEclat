package distEclat;

public interface SetReporter {
	  public void report(int[] itemset, int support);
	  
	  public void close();
	}
