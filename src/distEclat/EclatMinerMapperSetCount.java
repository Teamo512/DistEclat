package distEclat;

import org.apache.hadoop.io.LongWritable;


public class EclatMinerMapperSetCount extends EclatMinerMapperBase<LongWritable> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new ItemsetLengthCountReporter(context);
  }
}
