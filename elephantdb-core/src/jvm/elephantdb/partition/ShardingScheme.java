package elephantdb.partition;

import java.io.Serializable;

public interface ShardingScheme extends Serializable {
    int shardIndex(byte[] shardKey, int shardCount);
}
