package elephantdb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

// Can we make an interface out of this?
public class DomainSpec implements Writable, Serializable {
    public static final Logger LOG = Logger.getLogger(DomainSpec.class);

    private int _numShards;
    private PersistenceCoordinator _coordinator;
    private ShardScheme _shardScheme;
    private List<List<String>> _kryoPairs;

    public static final  String DOMAIN_SPEC_FILENAME = "domain-spec.yaml";
    private static final String LOCAL_PERSISTENCE_CONF = "local_persistence";
    private static final String SHARD_SCHEME_CONF = "shard_scheme";

    private static final String NUM_SHARDS_CONF = "num_shards";
    private static final String SERIALIZER_CONF = "kryo_pairs";

    public DomainSpec() {
    }

    /**
     * Here's the big daddy.
     * @param factClass String name of the class we'll use to instantiate new persistences.
     * @param numShards
     */
    public DomainSpec(String factClass, String shardSchemeClass, int numShards) {
        this(factClass, shardSchemeClass, numShards, new ArrayList<List<String>>());
    }

    public DomainSpec(String factClass, String shardSchemeClass, int numShards, List<List<String>> kryoPairs) {
        this(Utils.classForName(factClass), Utils.classForName(shardSchemeClass), numShards, kryoPairs);
    }

    public DomainSpec(Class factClass, Class shardSchemeClass, int numShards) {
        this(factClass, shardSchemeClass, numShards, new ArrayList<List<String>>());
    }

    public DomainSpec(Class factClass, Class shardSchemeClass, int numShards, List<List<String>> kryoPairs) {
        this((PersistenceCoordinator)Utils.newInstance(factClass),
            (ShardScheme)Utils.newInstance(shardSchemeClass),
            numShards, kryoPairs);
    }

    public DomainSpec(PersistenceCoordinator coordinator, ShardScheme shardScheme, int numShards) {
        this(coordinator, shardScheme, numShards, new ArrayList<List<String>>());
    }

    public DomainSpec(PersistenceCoordinator coordinator, ShardScheme shardScheme, int numShards,
                      List<List<String>> kryoPairs) {
        this._numShards = numShards;
        this._kryoPairs = kryoPairs;
        this._coordinator = coordinator;
        this._shardScheme = shardScheme;
    }

    public int getNumShards() {
        return _numShards;
    }

    public List<List<String>> getKryoPairs() {
        return _kryoPairs;
    }

    public PersistenceCoordinator getCoordinator() {
        if (_coordinator.getKryoPairs() != this.getKryoPairs())
            _coordinator.setKryoPairs(this.getKryoPairs());
        return _coordinator;
    }

    /*
    Wrappers for Persistence Coordinator functions.
     */
    
    private String shardPath(String root, int shardIdx) {
        return root + "/" + shardIdx;
    }

    public LocalPersistence openShardForAppend(String root, int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(root, shardIdx), new HashMap());
    }

    public LocalPersistence openShardForRead(String root, int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(root, shardIdx), new HashMap());
    }

    public LocalPersistence createShard(String root, int shardIdx) throws IOException {
        return getCoordinator().openPersistenceForAppend(shardPath(root, shardIdx), new HashMap());
    }

    /*
    The following functions deal with the shard scheme; when you access a shard scheme through the domain it becomes possible to wrap certain functionality.
     */
    public ShardScheme getShardScheme() {
        _shardScheme.setKryoPairs(this.getKryoPairs());
        return _shardScheme;
    }
    
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
        List<List<String>> kryoMap = (List<List<String>>) specmap.get(SERIALIZER_CONF);

        return new DomainSpec(persistenceConf, shardSchemeConf, numShards, kryoMap);
    }

    public void writeToStream(OutputStream os) {
        YAML.dump(mapify(), new OutputStreamWriter(os));
    }

    private Map<String, Object> mapify() {
        Map<String, Object> spec = new HashMap<String, Object>();
        spec.put(LOCAL_PERSISTENCE_CONF, _coordinator.getClass().getName());
        spec.put(SHARD_SCHEME_CONF, _shardScheme.getClass().getName());
        spec.put(NUM_SHARDS_CONF, _numShards);
        spec.put(SERIALIZER_CONF, _kryoPairs);
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
        this._kryoPairs = spec._kryoPairs;
    }
}
