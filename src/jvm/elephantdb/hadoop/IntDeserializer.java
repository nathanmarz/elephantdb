package elephantdb.hadoop;

import elephantdb.Utils;
import org.apache.hadoop.io.BytesWritable;

public class IntDeserializer implements Deserializer {
    public Object deserialize(BytesWritable bw) {
        return Utils.deserializeInt(bw);
    }
}
