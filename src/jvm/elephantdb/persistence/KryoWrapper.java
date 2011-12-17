package elephantdb.persistence;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * User: sritchie
 * Date: 12/9/11
 * Time: 11:15 AM
 */
public abstract class KryoWrapper implements Serializable {
    public static final Logger LOG = Logger.getLogger(KryoWrapper.class);
    private transient KryoBuffer _kryoBuf;

    public void setKryoPairs(List<List<String>> pairs) {
        getKryoBuffer().setKryoPairs(pairs);
    }

    public List<List<String>> getKryoPairs() {
        return getKryoBuffer().getKryoPairs();
    }

    public KryoBuffer getKryoBuffer() {
        if (_kryoBuf == null) {
            _kryoBuf = new KryoBuffer();
        }

        return _kryoBuf;
    }

    public class KryoBuffer implements Serializable {
        private transient ObjectBuffer _objectBuffer;
        private List<List<String>> _kryoPairs = new ArrayList<List<String>>();
        private List<List<String>> _embeddedPairs = new ArrayList<List<String>>();


        private ObjectBuffer makeObjectBuffer() {
            Kryo k = new Kryo();
            KryoFactory.populateKryo(k, getKryoPairs(), false, true);
            return KryoFactory.newBuffer(k);
        }

        public void setKryoPairs(List<List<String>> pairs) {
            _kryoPairs = pairs;
        }

        public List<List<String>> getKryoPairs() {
            return _kryoPairs;
        }

        private void ensureObjectBuf() {
            if (_objectBuffer == null || _kryoPairs != _embeddedPairs) {
                _embeddedPairs = _kryoPairs;
                _objectBuffer = makeObjectBuffer();
            }
        }

        private ObjectBuffer getKryoBuffer() {
            ensureObjectBuf();
            return _objectBuffer;
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
}
