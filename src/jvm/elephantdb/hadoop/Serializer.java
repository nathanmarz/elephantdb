package elephantdb.hadoop;

import org.apache.hadoop.io.BytesWritable;

import java.io.Serializable;

public interface Serializer extends Serializable {
    public byte[] serialize(Object k);
}
