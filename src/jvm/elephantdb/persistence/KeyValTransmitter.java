package elephantdb.persistence;

import elephantdb.hadoop.Common;
import org.apache.hadoop.io.BytesWritable;

import java.util.Map;

/** User: sritchie Date: 11/22/11 Time: 4:05 PM */
public class KeyValTransmitter implements Transmitter {
    Map _opts = null;

    public KeyValTransmitter() {
        this(null);
    }

    public KeyValTransmitter(Map opts) {
        _opts = opts;
    }

    public byte[] serializeKey(Object key) {
        return Common.serializeElephantVal(key);
    }
    
    public Object deserializeKey(byte[] key) {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    public byte[] serializeVal(Object val) {
        return ((BytesWritable) val).getBytes();
    }

    public Object deserializeVal(byte[]val) {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
