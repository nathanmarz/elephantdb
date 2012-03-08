package elephantdb.serialize;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
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

    private transient ObjectBuffer objectBuffer;
    private Iterable<KryoFactory.ClassPair> kryoPairs = new ArrayList<KryoFactory.ClassPair>();

    public KryoSerializer() {
    }

    public KryoSerializer(Iterable<KryoFactory.ClassPair> kryoPairs) {
        setKryoPairs(kryoPairs);
    }

    private ObjectBuffer makeObjectBuffer() {
        Kryo k = new Kryo();
        KryoFactory factory = new KryoFactory(new Configuration());
        k.setRegistrationOptional(true);
        factory.registerBasic(k, getKryoPairs());
        return KryoFactory.newBuffer(k);
    }

    public void setKryoPairs(Iterable<KryoFactory.ClassPair> pairs) {
        kryoPairs = pairs;
    }

    public Iterable<KryoFactory.ClassPair> getKryoPairs() {
        return kryoPairs;
    }

    private ObjectBuffer getKryoBuffer() {
        if (objectBuffer == null)
            objectBuffer = makeObjectBuffer();

        return objectBuffer;
    }

    public byte[] serialize(Object o) {
        LOG.debug("Serializing object: " + o);
        return getKryoBuffer().writeClassAndObject(o);
    }

    public Object deserialize(byte[] bytes) {
        return getKryoBuffer().readClassAndObject(bytes);
    }

    public <T> T deserialize(byte[] bytes, Class<T> klass) {
        return getKryoBuffer().readObject(bytes, klass);
    }
}
