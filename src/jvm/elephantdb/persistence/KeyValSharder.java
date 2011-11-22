package elephantdb.persistence;

import elephantdb.hadoop.Common;

import java.math.BigInteger;
import java.util.Map;

import elephantdb.Utils;

/** User: sritchie Date: 11/22/11 Time: 5:32 PM */
public class KeyValSharder implements Sharder {
    Map _opts = null;

    public KeyValSharder() {
        this(null);
    }

    public KeyValSharder(Map opts) {
        _opts = opts;
    }

    public int shardIndex(int numShards, Object key, Object val) {
        byte[] serkey = Common.serializeElephantVal(key);

        BigInteger hash = new BigInteger(Utils.md5Hash(serkey));
        return hash.mod(new BigInteger("" + numShards)).intValue();
    }
}
