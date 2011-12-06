package elephantdb;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import elephantdb.persistence.LocalPersistenceFactory;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.jvyaml.YAML;
import sun.awt.image.ImageWatched;


// Can we make an interface out of this?

public class DomainSpec implements Writable, Serializable {
    private int _numShards;
    private LocalPersistenceFactory _localFact;
    private LinkedHashMap<String, String> _kryoPairs;

    public static final  String DOMAIN_SPEC_FILENAME = "domain-spec.yaml";
    private static final String LOCAL_PERSISTENCE_CONF = "local_persistence";
    private static final String NUM_SHARDS_CONF = "num_shards";
    private static final String SERIALIZER_CONF = "kryo_pairs";

    public DomainSpec() {
    }

    /**
     * Here's the big daddy.
     * @param factClass String name of the class we'll use to instantiate new persistences.
     * @param numShards
     */
    public DomainSpec(String factClass, int numShards) {
        this(factClass, numShards, new LinkedHashMap<String, String>());
    }

    public DomainSpec(String factClass, int numShards, LinkedHashMap<String, String> kryoPairs) {
        this(Utils.classForName(factClass), numShards, kryoPairs);
    }

    public DomainSpec(Class factClass, int numShards) {
        this(factClass, numShards, new LinkedHashMap<String, String>());
    }

    public DomainSpec(Class factClass, int numShards, LinkedHashMap<String, String> kryoPairs) {
        this((LocalPersistenceFactory)Utils.newInstance(factClass), numShards, kryoPairs);
    }

    public DomainSpec(LocalPersistenceFactory localFact, int numShards) {
        this(localFact, numShards, new LinkedHashMap<String, String>());
    }

    public DomainSpec(LocalPersistenceFactory localFact, int numShards,  LinkedHashMap<String, String> kryoPairs) {
        this._localFact = localFact;
        this._numShards = numShards;
        this._kryoPairs = kryoPairs;
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

    public LocalPersistenceFactory getLPFactory() {
        return _localFact;
    }

    public LinkedHashMap<String, String> getKryoPairs() {
        return _kryoPairs;
    }

    public ObjectBuffer getObjectBuffer() {
        Kryo k = new Kryo();
        KryoFactory.populateKryo(k, getKryoPairs(), false, false);
        return KryoFactory.newBuffer(k);
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
        int numShards = ((Long)specmap.get(NUM_SHARDS_CONF)).intValue();
        LinkedHashMap<String, String> kryoMap = (LinkedHashMap<String, String>) specmap.get(SERIALIZER_CONF);

        return new DomainSpec(persistenceConf, numShards, kryoMap);
    }

    public void writeToStream(OutputStream os) {
        YAML.dump(mapify(), new OutputStreamWriter(os));
    }

    private Map<String, Object> mapify() {
        Map<String, Object> spec = new HashMap<String, Object>();
        spec.put(LOCAL_PERSISTENCE_CONF, _localFact.getClass().getName());
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
        this._localFact = spec._localFact;
    }
}
