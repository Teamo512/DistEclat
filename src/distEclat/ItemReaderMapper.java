package distEclat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemReaderMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
	  
	  public static final String tidsDelimiter = " ";
	  public static final String tidsFileDelimiter = "\t";
	  
	  private long minSup;
	  
	  @Override
	  public void setup(Context context) {
	    Configuration conf = context.getConfiguration();
	    
	    minSup = conf.getLong("minsup", 1);
	  }
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String[] split = value.toString().split(tidsFileDelimiter);
	    String item = split[0];
	    String[] tids = split[1].split(tidsDelimiter);
	    
	    if (tids.length < minSup) {
	      return;
	    }
	    
	    IntWritable[] iw = new IntWritable[tids.length];
	    int ix = 0;
	    for (String stringTid : tids) {
	      iw[ix++] = new IntWritable(Integer.parseInt(stringTid));
	    }
	    
	    context.write(new Text(item), new IntArrayWritable(iw));
	  }
	}
