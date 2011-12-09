package elephantdb.persistence;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * User: sritchie
 * Date: 12/9/11
 * Time: 11:15 AM
 *
 * TODO: Give KryoWrapper the ability to accept a list of KryoSerializations.
 */
public abstract class KryoWrapper {
    public static final Logger LOG = Logger.getLogger(KryoWrapper.class);

    ObjectBuffer _kryoBuf;
    private List<List<String>> _kryoPairs;

    public static class KryoBuffer {
        private static ObjectBuffer _kryoBuf;
        private static List<List<String>> _kryoPairs = new ArrayList<List<String>>();

        public static ObjectBuffer getObjectBuffer(List<List<String>> pairs) {
            if (_kryoBuf == null || pairs != _kryoPairs) {
                Kryo k = new Kryo();
                if (pairs != null) _kryoPairs = pairs;
                KryoFactory.populateKryo(k, _kryoPairs, false, true);
                _kryoBuf = KryoFactory.newBuffer(k);
            }
            return _kryoBuf;
        }
    }

    public void setKryoPairs(List<List<String>> pairs) {
        _kryoPairs = pairs;
    }

    public List<List<String>> getKryoPairs() {
        return _kryoPairs;
    }

    public ObjectBuffer getObjectBuffer() {
        return KryoBuffer.getObjectBuffer(getKryoPairs());
    }

    private void ensureKryoBuf() {
        if (_kryoBuf == null) {
            _kryoBuf = getObjectBuffer();
        }
    }

    public byte[] serialize(Object o) {
        ensureKryoBuf();
        LOG.debug("Serializing object: " + o);
        return _kryoBuf.writeClassAndObject(o);
    }

    public Object deserialize(byte[] bytes) {
        ensureKryoBuf();
        return _kryoBuf.readClassAndObject(bytes);
    }

    public <T> T deserialize(byte[] bytes, Class<T> klass) {
        ensureKryoBuf();
        return _kryoBuf.readObject(bytes, klass);
    }
}
