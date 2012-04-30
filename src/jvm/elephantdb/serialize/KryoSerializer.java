package elephantdb.serialize;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 12:22 PM
 */
public class KryoSerializer implements Serializer {
    public static final Logger LOG = Logger.getLogger(KryoSerializer.class);

    /**
     * Initial capacity of the Kryo object buffer.
     */
    private static final int INIT_CAPACITY = 2000;

    /**
     * Maximum capacity of the Kryo object buffer.
     */
    private static final int FINAL_CAPACITY = 2000000000;

    private transient Output output;
    private transient Kryo kryo;
    private Iterable<KryoFactory.ClassPair> kryoPairs = new ArrayList<KryoFactory.ClassPair>();

    public KryoSerializer() {
    }

    public KryoSerializer(Iterable<KryoFactory.ClassPair> kryoPairs) {
        setKryoPairs(kryoPairs);
    }

    private Kryo freshKryo() {
        Kryo k = new Kryo();
        KryoFactory factory = new KryoFactory(new Configuration());
        k.setRegistrationRequired(false);
        factory.registerBasic(k, getKryoPairs());
        return k;
    }

    public void setKryoPairs(Iterable<KryoFactory.ClassPair> pairs) {
        kryoPairs = pairs;
    }

    public Iterable<KryoFactory.ClassPair> getKryoPairs() {
        return kryoPairs;
    }

    public Kryo getKryo() {
        if (kryo == null)
            kryo = freshKryo();

        return kryo;
    }

    public Output getOutput() {
        if (output == null)
            output = new Output(INIT_CAPACITY, FINAL_CAPACITY);

        return output;
    }

    public byte[] serialize(Object o) {
        LOG.debug("Serializing object: " + o);
        Output output = getOutput();
        output.clear();
        freshKryo().writeClassAndObject(output, o);
        return output.toBytes();
    }

    public Object deserialize(byte[] bytes) {
        return freshKryo().readClassAndObject(new Input(bytes));
    }

    public <T> T deserialize(byte[] bytes, Class<T> klass) {
        return freshKryo().readObject(new Input(bytes), klass);
    }
}
