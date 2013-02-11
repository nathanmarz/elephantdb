package elephantdb.persistence;

import java.io.IOException;

public interface ShardSet {
    int getNumShards();
    int shardIndex(Object shardKey);
    String shardPath(int shardIdx);
    Persistence openShardForAppend(int shardIdx) throws IOException;
    Persistence openShardForRead(int shardIdx) throws IOException;
    Persistence createShard(int shardIdx) throws IOException;
}
