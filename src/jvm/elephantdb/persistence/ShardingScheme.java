package elephantdb.persistence;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 2:57 PM
 */
public interface ShardingScheme {
    int shardIndex(Object shardKey, int shardCount);
}
