package elephantdb.persistence;

import java.io.IOException;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 3:09 PM
 */
public interface ShardSet {
    Persistence openShardForAppend(int shardIdx) throws IOException;
    Persistence openShardForRead(int shardIdx) throws IOException;
    Persistence createShard(int shardIdx) throws IOException;
    int getNumShards();
    String shardPath(int shardIdx);
}
