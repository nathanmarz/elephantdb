package elephantdb.persistence;

import java.math.BigInteger;

import elephantdb.Utils;

/** User: sritchie Date: 11/22/11 Time: 5:32 PM */
public class HashModScheme extends ShardScheme {
    public int shardIndex(Object shardKey, int shardCount) {
        byte[] serkey = getKryoBuffer().serialize(shardKey);
        BigInteger hash = new BigInteger(Utils.md5Hash(serkey));
        return hash.mod(new BigInteger("" + shardCount)).intValue();
    }
}
