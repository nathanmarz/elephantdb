package elephantdb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import elephantdb.persistence.KryoWrapper;
import elephantdb.persistence.LocalPersistence;
import elephantdb.persistence.PersistenceCoordinator;
import elephantdb.persistence.ShardScheme;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.jvyaml.YAML;

public class DomainSpec implements Writable, Serializable {
    public static final Logger LOG = Logger.getLogger(DomainSpec.class);
    public static final  String DOMAIN_SPEC_FILENAME = "domain-spec.yaml";

    private static final String LOCAL_PERSISTENCE_CONF = "local_persistence";
    private static final String SHARD_SCHEME_CONF = "shard_scheme";
    private static final String NUM_SHARDS_CONF = "num_shards";
    private static final String KRYO_PAIRS = "kryo_pairs";
    private static final String PERSISTENCE_OPTS = "persistence_opts";


    // This gets serialized in via the conf.
    public static class Args implements Serializable {
        private List<List<String>> kryoPairs = new ArrayList<List<String>>();
        private Map persistenceOptions = new HashMap();
    }

    Args _optionalArgs;

    private int _numShards;
    private PersistenceCoordinator _coordinator;
    private ShardScheme _shardScheme;

    public DomainSpec() {
    }

    /**
     * Here's the big daddy.
     * @param factClass String name of the class we'll use to instantiate new persistences.
     * @param numShards
     */
    public DomainSpec(String factClass, String shardSchemeClass, int numShards) {
        this(factClass, shardSchemeClass, numShards, new Args());
    }

    public DomainSpec(String factClass, String shardSchemeClass, int numShards, Args args) {
        this(Utils.classForName(factClass), Utils.classForName(shardSchemeClass), numShards, args);
    }

    public DomainSpec(Class factClass, Class shardSchemeClass, int numShards) {
        this(factClass, shardSchemeClass, numShards, new Args());
    }

    public DomainSpec(Class factClass, Class shardSchemeClass, int numShards, Args args) {
        this((PersistenceCoordinator)Utils.newInstance(factClass),
                (ShardScheme)Utils.newInstance(shardSchemeClass),
                numShards, args);
    }

    public DomainSpec(PersistenceCoordinator coordinator, ShardScheme shardScheme, int numShards) {
        this(coordinator, shardScheme, numShards, new Args());
    }

    public DomainSpec(PersistenceCoordinator coordinator, ShardScheme shardScheme, int numShards, Args args) {
        this._numShards = numShards;
        this._coordinator = coordinator;
        this._shardScheme = shardScheme;
        this._optionalArgs = args;
    }

    public int getNumShards() {
        return _numShards;
    }

    public List<List<String>> getKryoPairs() {
        return _optionalArgs.kryoPairs;
    }

    public Map getPersistenceOptions() {
        return _optionalArgs.persistenceOptions;
    }

    public void ensureMatchingPairs(KryoWrapper wrapper) {
        if (wrapper.getKryoPairs() != this.getKryoPairs())
            wrapper.setKryoPairs(this.getKryoPairs());
    }

    public PersistenceCoordinator getCoordinator() {
        ensureMatchingPairs(_coordinator);
        return _coordinator;
    }

    public ShardScheme getShardScheme() {
        ensureMatchingPairs(_shardScheme);
        return _shardScheme;
    }

    /*
    Wrappers for Persistence Coordinator functions.
     */

    public void assertValidShard(int shardIdx) {
        if ( !(shardIdx >= 0 && shardIdx < getNumShards()) ) {
            String errorStr = shardIdx +
                    " is not a valid shard index. Index must be between 0 and " + (getNumShards() - 1);
            throw new RuntimeException(errorStr);
        }
    }
    private String shardPath(String root, int shardIdx) {
        assertValidShard(shardIdx);
        return root + "/" + shardIdx;
    }

    public LocalPersistence openShardForAppend(String root, int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(root, shardIdx), getPersistenceOptions());
    }

    public LocalPersistence openShardForRead(String root, int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(root, shardIdx), getPersistenceOptions());
    }

    public LocalPersistence createShard(String root, int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(root, shardIdx), getPersistenceOptions());
    }

    /*
    The following functions deal with the shard scheme; when you access a shard scheme through the domain it becomes possible to wrap certain functionality.
     */
    
    public int shardIndex(Object shardKey) {
        return getShardScheme().shardIndex(shardKey, getNumShards());
    }

    /*
    Back to the good old DomainSpec business.
     */

    @Override public String toString() {
        return mapify().toString();
    }

    @Override public boolean equals(Object other) {
        DomainSpec o = (DomainSpec) other;
        return mapify().equals(o.mapify());
    }

    @Override public int hashCode() {
        return mapify().hashCode();
    }

    public static DomainSpec readFromFileSystem(FileSystem fs, String dirpath) throws IOException {
        Path filePath = new Path(dirpath + "/" + DOMAIN_SPEC_FILENAME);
        if(!fs.exists(filePath)) {
            return null;
        }

        FSDataInputStream is = fs.open(filePath);
        DomainSpec ret = parseFromStream(is);
        is.close();
        return ret;
    }

    public static boolean exists(FileSystem fs, String dirpath) throws IOException {
        return fs.exists(new Path(dirpath + "/" + DOMAIN_SPEC_FILENAME));
    }

    public static DomainSpec parseFromStream(InputStream is) {
        Map format = (Map) YAML.load(new InputStreamReader(is));
        return parseFromMap(format);
    }

    @SuppressWarnings("unchecked")
    protected static DomainSpec parseFromMap(Map<String, Object> specmap) {
        String persistenceConf = (String)specmap.get(LOCAL_PERSISTENCE_CONF);
        String shardSchemeConf = (String)specmap.get(SHARD_SCHEME_CONF);
        int numShards = ((Long)specmap.get(NUM_SHARDS_CONF)).intValue();

        Args args = new Args();
        args.persistenceOptions = (Map) specmap.get(PERSISTENCE_OPTS);
        args.kryoPairs = (List<List<String>>) specmap.get(KRYO_PAIRS);

        return new DomainSpec(persistenceConf, shardSchemeConf, numShards, args);
    }

    public void writeToStream(OutputStream os) {
        YAML.dump(mapify(), new OutputStreamWriter(os));
    }

    private Map<String, Object> mapify() {
        Map<String, Object> spec = new HashMap<String, Object>();
        spec.put(LOCAL_PERSISTENCE_CONF, _coordinator.getClass().getName());
        spec.put(SHARD_SCHEME_CONF, _shardScheme.getClass().getName());
        spec.put(NUM_SHARDS_CONF, _numShards);
        spec.put(KRYO_PAIRS, getKryoPairs());
        spec.put(PERSISTENCE_OPTS, getPersistenceOptions());
        return spec;
    }

    public void writeToFileSystem(FileSystem fs, String dirpath) throws IOException {
        fs.mkdirs(new Path(dirpath));
        FSDataOutputStream os = fs.create(new Path(dirpath + "/" + DOMAIN_SPEC_FILENAME));
        writeToStream(os);
        os.close();
    }

    public void write(DataOutput d) throws IOException {
        String ser = YAML.dump(mapify());
        WritableUtils.writeString(d, ser);
    }

    public void readFields(DataInput di) throws IOException {
        DomainSpec spec = parseFromMap((Map<String, Object>) YAML.load(WritableUtils.readString(di)));
        this._numShards = spec._numShards;
        this._coordinator = spec._coordinator;
        this._shardScheme = spec._shardScheme;
        this._optionalArgs = spec._optionalArgs;
    }
}
