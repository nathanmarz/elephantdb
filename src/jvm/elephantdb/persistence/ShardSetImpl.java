package elephantdb.persistence;

import elephantdb.DomainSpec;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 3:13 PM
 */
public class ShardSetImpl implements ShardSet {
    
    private String root;
    private int numShards;
    private Coordinator coordinator;
    private ShardingScheme shardingScheme;
    private DomainSpec.Args optionalArgs;

    public ShardSetImpl(Coordinator coordinator, ShardingScheme shardingScheme, String root, int numShards) {
        this(coordinator, shardingScheme, root, numShards, new DomainSpec.Args());
    }
    
    public ShardSetImpl(Coordinator coordinator, ShardingScheme shardingScheme, String root, int numShards, DomainSpec.Args args) {
        this.root = root;
        this.numShards = numShards;
        this.coordinator = coordinator;
        this.shardingScheme = shardingScheme;
        this.optionalArgs = args;
    }

    public void ensureMatchingPairs(KryoWrapper wrapper) {
        if (wrapper.getKryoPairs() != this.getKryoPairs())
            wrapper.setKryoPairs(this.getKryoPairs());
    }

    public Coordinator getCoordinator() {
        // TODO: Remove this cast, as the Coordinator will be prepared.
        ensureMatchingPairs((KryoWrapper) coordinator);
        return coordinator;
    }

    public ShardingScheme getShardScheme() {
        // TODO: Remove this cast, as the IShardScheme will be prepared.
        ensureMatchingPairs((KryoWrapper) shardingScheme);
        return shardingScheme;
    }

    public List<List<String>> getKryoPairs() {
        return optionalArgs.kryoPairs;
    }

    public Map getPersistenceOptions() {
        return optionalArgs.persistenceOptions;
    }

    public int getNumShards() {
        return numShards;
    }

    public void assertValidShard(int shardIdx) {
        if ( !(shardIdx >= 0 && shardIdx < getNumShards())) {
            String errorStr = shardIdx +
                    " is not a valid shard index. Index must be between 0 and " + (getNumShards() - 1);
            throw new RuntimeException(errorStr);
        }
    }

    private String shardPath(int shardIdx) {
        assertValidShard(shardIdx);
        return root + "/" + shardIdx;
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
}
