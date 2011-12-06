package elephantdb.persistence;

import com.esotericsoftware.kryo.ObjectBuffer;
import elephantdb.DomainSpec;
import elephantdb.hadoop.Common;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import elephantdb.Utils;

/** User: sritchie Date: 11/22/11 Time: 5:32 PM */
public class HashModScheme extends ShardScheme {

    public int shardIndex(Object shardKey) {
        assertSpec(); // check that spec exists.

        byte[] serkey = _spec.serialize(shardKey);
        int shardCount = _spec.getNumShards();
        
        BigInteger hash = new BigInteger(Utils.md5Hash(serkey));
        return hash.mod(new BigInteger("" + shardCount)).intValue();
    }
}
