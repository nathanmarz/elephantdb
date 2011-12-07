package elephantdb;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import elephantdb.persistence.PersistenceCoordinator;
import elephantdb.persistence.ShardScheme;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.jvyaml.YAML;


// Can we make an interface out of this?
public class DomainSpec implements Writable, Serializable {
    private int _numShards;
    private PersistenceCoordinator _coordinator;
    private ShardScheme _shardScheme;
    private LinkedHashMap<String, String> _kryoPairs;
    private ObjectBuffer _kryoBuf;

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
        this(factClass, shardSchemeClass, numShards, new LinkedHashMap<String, String>());
    }

    public DomainSpec(String factClass, String shardSchemeClass, int numShards, LinkedHashMap<String, String> kryoPairs) {
        this(Utils.classForName(factClass), Utils.classForName(shardSchemeClass), numShards, kryoPairs);
    }

    public DomainSpec(Class factClass, Class shardSchemeClass, int numShards) {
        this(factClass, shardSchemeClass, numShards, new LinkedHashMap<String, String>());
    }

    public DomainSpec(Class factClass, Class shardSchemeClass, int numShards, LinkedHashMap<String, String> kryoPairs) {
        this((PersistenceCoordinator)Utils.newInstance(factClass),
            (ShardScheme)Utils.newInstance(shardSchemeClass),
            numShards, kryoPairs);
    }

    public DomainSpec(PersistenceCoordinator coordinator, ShardScheme shardScheme, int numShards) {
        this(coordinator, shardScheme, numShards, new LinkedHashMap<String, String>());
    }

    public DomainSpec(PersistenceCoordinator coordinator, ShardScheme shardScheme, int numShards,  LinkedHashMap<String, String> kryoPairs) {
        this._numShards = numShards;
        this._kryoPairs = kryoPairs;
        this._kryoBuf = getObjectBuffer();

        coordinator.setSpec(this);
        this._coordinator = coordinator;

        shardScheme.setSpec(this);
        this._shardScheme = shardScheme;
    }

    @Override
    public String toString() {
        return mapify().toString();
    }

    @Override
    public boolean equals(Object other) {
        DomainSpec o = (DomainSpec) other;
        return mapify().equals(o.mapify());
    }

    @Override
    public int hashCode() {
        return mapify().hashCode();
    }

    public int getNumShards() {
        return _numShards;
    }

    public PersistenceCoordinator getCoordinator() {
        return _coordinator;
    }

    public ShardScheme getShardScheme() {
        return _shardScheme;
    }

    public LinkedHashMap<String, String> getKryoPairs() {
        return _kryoPairs;
    }

    public ObjectBuffer getObjectBuffer() {
        Kryo k = new Kryo();
        KryoFactory.populateKryo(k, getKryoPairs(), false, false);
        return KryoFactory.newBuffer(k);
    }

    public byte[] serialize(Object o) {
        return _kryoBuf.writeClassAndObject(o);
    }

    public Object deserialize(byte[] bytes) {
        return _kryoBuf.readClassAndObject(bytes);
    }

    public <T> T deserialize(byte[] bytes, Class<T> klass) {
        return _kryoBuf.readObject(bytes, klass);
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
        LinkedHashMap<String, String> kryoMap = (LinkedHashMap<String, String>) specmap.get(SERIALIZER_CONF);

        return new DomainSpec(persistenceConf, shardSchemeConf, numShards, kryoMap);
    }

    public void writeToStream(OutputStream os) {
        YAML.dump(mapify(), new OutputStreamWriter(os));
    }

    private Map<String, Object> mapify() {
        Map<String, Object> spec = new HashMap<String, Object>();
        spec.put(LOCAL_PERSISTENCE_CONF, _coordinator.getClass().getName());
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
        DomainSpec spec = parseFromMap((Map<String, Object>)YAML.load(WritableUtils.readString(di)));
        this._numShards = spec._numShards;
        this._coordinator = spec._coordinator;
        this._shardScheme = spec._shardScheme;
    }
}
