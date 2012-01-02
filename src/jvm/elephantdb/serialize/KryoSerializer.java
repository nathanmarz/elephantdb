package elephantdb.serialize;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 12:22 PM
 */
public class KryoSerializer implements Serializer {
    public static final Logger LOG = Logger.getLogger(KryoSerializer.class);

    private transient ObjectBuffer objectBuffer;
    private List<List<String>> kryoPairs = new ArrayList<List<String>>();

    public KryoSerializer() {
    }

    public KryoSerializer(List<List<String>> kryoPairs) {
        setKryoPairs(kryoPairs);
    }

    private ObjectBuffer makeObjectBuffer() {
        Kryo k = new Kryo();
        KryoFactory.populateKryo(k, getKryoPairs(), false, true);
        return KryoFactory.newBuffer(k);
    }

    public void setKryoPairs(List<List<String>> pairs) {
        kryoPairs = pairs;
    }

    public List<List<String>> getKryoPairs() {
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
