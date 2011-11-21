package elephantdb.hadoop;

import elephantdb.Utils;
import org.apache.hadoop.io.BytesWritable;

/** User: sritchie Date: 11/20/11 Time: 7:51 PM */
public class Common {
    public static byte[] getBytes(BytesWritable bw) {
        byte[] padded = bw.getBytes();
        byte[] ret = new byte[bw.getLength()];
        System.arraycopy(padded, 0, ret, 0, ret.length);
        return ret;
    }

    public static byte[] serializeElephantVal(Object o) {
        if (o instanceof BytesWritable) {
            return getBytes((BytesWritable) o);
        } else if (o instanceof String) {
            return Utils.serializeString((String) o);
        } else if (o instanceof Integer) {
            return Utils.serializeInt((Integer) o);
        } else if (o instanceof Long) {
            return Utils.serializeLong((Long) o);
        }
        throw new IllegalArgumentException("Could not serialize " + o.toString());
    }
}
