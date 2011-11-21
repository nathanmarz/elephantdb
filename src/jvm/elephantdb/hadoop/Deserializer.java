package elephantdb.hadoop;

import org.apache.hadoop.io.BytesWritable;

import java.io.Serializable;

public interface Deserializer extends Serializable {
    public Object deserialize(BytesWritable bw);
}