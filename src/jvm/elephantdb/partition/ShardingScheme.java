package elephantdb.partition;

import java.io.Serializable;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 2:57 PM
 */
public interface ShardingScheme extends Serializable {
    int shardIndex(Object shardKey, int shardCount);
}
