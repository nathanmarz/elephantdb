package elephantdb.serialize;

import cascading.kryo.KryoFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 12:22 PM
 */
public class KryoSerializer implements Serializer {
    public static final Logger LOG = Logger.getLogger(KryoSerializer.class);
    
    private static final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>();
    private static final ThreadLocal<ByteArrayOutputStream> byteStream = new ThreadLocal<ByteArrayOutputStream>();
    private Iterable<KryoFactory.ClassPair> kryoPairs = new ArrayList<KryoFactory.ClassPair>();

    public KryoSerializer() {
    }

    public KryoSerializer(Iterable<KryoFactory.ClassPair> kryoPairs) {
        setKryoPairs(kryoPairs);
    }

    private Kryo freshKryo() {
        Kryo k = new Kryo();
        KryoFactory factory = new KryoFactory(new Configuration());
        k.setInstantiatorStrategy(new StdInstantiatorStrategy());
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
        if (kryo.get() == null)
            kryo.set(freshKryo());

        return kryo.get();
    }
    
    public ByteArrayOutputStream getByteStream() {
        if (byteStream.get() == null)
            byteStream.set(new ByteArrayOutputStream());

        return byteStream.get();
    }

    public byte[] serialize(Object o) {
        getByteStream().reset();
        Output ko = new Output(getByteStream());
        getKryo().writeClassAndObject(ko, o);
        ko.flush();
        byte[] bytes = ko.toBytes();
        
        return bytes;
    }

    public Object deserialize(byte[] bytes) {
        return getKryo().readClassAndObject(new Input(bytes));
    }

    public <T> T deserialize(byte[] bytes, Class<T> klass) {
        return getKryo().readObject(new Input(bytes), klass);
    }
}
