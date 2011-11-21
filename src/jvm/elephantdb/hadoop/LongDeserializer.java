package elephantdb.hadoop;

import elephantdb.Utils;
import org.apache.hadoop.io.BytesWritable;

/** User: sritchie Date: 11/20/11 Time: 7:43 PM */
public class LongDeserializer implements Deserializer {
    public Object deserialize(BytesWritable bw) {
        return Utils.deserializeLong(bw);
    }
}
