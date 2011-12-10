package elephantdb.persistence;

import java.io.Serializable;

/** User: sritchie Date: 12/6/11 Time: 2:06 PM */
public abstract class ShardScheme extends KryoWrapper {
    public abstract int shardIndex(Object shardKey, int shardCount);
}