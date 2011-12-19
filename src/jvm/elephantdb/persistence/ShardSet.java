package elephantdb.persistence;

import java.io.IOException;
import java.util.List;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 3:09 PM
 */
public interface ShardSet {
    int getNumShards();
    int shardIndex(Object shardKey);
    String shardPath(int shardIdx);
    Persistence openShardForAppend(int shardIdx) throws IOException;
    Persistence openShardForRead(int shardIdx) throws IOException;
    Persistence createShard(int shardIdx) throws IOException;
}
