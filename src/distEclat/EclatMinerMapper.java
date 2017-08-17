package distEclat;

import org.apache.hadoop.io.Text;


public class EclatMinerMapper extends EclatMinerMapperBase<Text> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new TreeStringReporter(context);
  }
}
