package elephantdb.persistence;

import elephantdb.DomainSpec;
import elephantdb.partition.ShardingScheme;

import java.io.IOException;
import java.util.Map;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 3:13 PM
 */
public class ShardSetImpl implements ShardSet {
    
    private String root;
    private DomainSpec spec;

    public ShardSetImpl(String root, DomainSpec spec) {
        this.root = root;
        this.spec = spec;
    }

    public Coordinator getCoordinator() {
        return spec.getCoordinator();
    }

    public ShardingScheme getShardScheme() {
        return spec.getShardScheme();
    }

    public Map getPersistenceOptions() {
        return spec.getPersistenceOptions();
    }

    public int getNumShards() {
        return spec.getNumShards();
    }

    public void assertValidShard(int shardIdx) {
        if ( !(shardIdx >= 0 && shardIdx < getNumShards())) {
            String errorStr = shardIdx +
                    " is not a valid shard index. Index must be between 0 and " + (getNumShards() - 1);
            throw new AssertionError(errorStr);
        }
    }

    public Persistence openShardForAppend(int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(shardIdx), getPersistenceOptions());
    }

    public Persistence openShardForRead(int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForRead(shardPath(shardIdx), getPersistenceOptions());
    }

    public Persistence createShard(int shardIdx) throws IOException {
        return getCoordinator().createPersistence(shardPath(shardIdx), getPersistenceOptions());
    }

    public String shardPath(int shardIdx) {
        assertValidShard(shardIdx);
        return root + "/" + shardIdx;
    }

    /*
   The following functions deal with the shard scheme; when you access a shard scheme through the domain
   it becomes possible to wrap certain functionality.
    */
    public int shardIndex(Object shardKey) {
        return getShardScheme().shardIndex(shardKey, getNumShards());
    }
}
