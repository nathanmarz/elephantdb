package elephantdb.partition;

import elephantdb.Utils;

public class HashModScheme implements ShardingScheme {

    public int shardIndex(byte[] shardKey, int shardCount) {
        return Utils.keyShard(shardKey, shardCount);
    }
}
