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

    private static final int OUTPUT_BUFFER_SIZE = 1<<12;
    private static final int MAX_OUTPUT_BUFFER_SIZE = 1<<24;

    private static final int TIDY_FACTOR = 1<<4;

    private static final int SWITCH_LIMIT = Math.max(MAX_OUTPUT_BUFFER_SIZE, MAX_OUTPUT_BUFFER_SIZE / TIDY_FACTOR);

    private transient Output output;
    private transient Kryo kryo;

    private int prevPosition;

    private Iterable<KryoFactory.ClassPair> kryoPairs = new ArrayList<KryoFactory.ClassPair>();

    public KryoSerializer() {
    }

    public KryoSerializer(Iterable<KryoFactory.ClassPair> kryoPairs) {
        setKryoPairs(kryoPairs);
    }

    public void tidyBuffer() {
        int currentPosition = output.position();

        // If the previous serialized object was large (greater than the switch size) and the current
        // object falls below the switch size, reset the buffer to be small again. If both objects
        // are small (or large), no reallocation occurs.
        if (prevPosition > SWITCH_LIMIT && currentPosition <= SWITCH_LIMIT)
            output.setBuffer(new byte[OUTPUT_BUFFER_SIZE], MAX_OUTPUT_BUFFER_SIZE);

        prevPosition = currentPosition;
        output.clear();
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
            output = new Output(OUTPUT_BUFFER_SIZE, MAX_OUTPUT_BUFFER_SIZE);

        tidyBuffer();

        return output;
    }

    public byte[] serialize(Object o) {
        LOG.debug("Serializing object: " + o);
        Output output = getOutput();
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
