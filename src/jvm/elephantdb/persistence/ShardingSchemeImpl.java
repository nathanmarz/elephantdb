package elephantdb.persistence;

/** User: sritchie Date: 12/6/11 Time: 2:06 PM */
public abstract class ShardingSchemeImpl extends KryoWrapper implements ShardingScheme {
    public abstract int shardIndex(Object shardKey, int shardCount);
}