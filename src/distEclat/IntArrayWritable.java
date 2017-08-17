package distEclat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
	  
	  public IntArrayWritable() {
	    super(IntWritable.class);
	  }
	  
	  public IntArrayWritable(IntWritable[] iw) {
	    this();
	    set(iw);
	  }
	  
	  @Override
	  public String toString() {
	    StringBuilder sb = new StringBuilder();
	    for (String s : super.toStrings()) {
	      sb.append(s).append(" ");
	    }
	    return sb.toString();
	  }
	}
