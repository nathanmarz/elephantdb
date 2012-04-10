package elephantdb;

import cascading.kryo.KryoFactory;
import elephantdb.persistence.Coordinator;
import elephantdb.serialize.KryoSerializer;
import elephantdb.serialize.SerializationWrapper;
import elephantdb.serialize.Serializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Utils {

    public static byte[] md5Hash(byte[] key) {
        try {
            return MessageDigest.getInstance("MD5").digest(key);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /** Resolves the supplied string into its class. Throws a runtime exception on failure. */
    public static Class classForName(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** generates a new instance of the supplied class. */
    public static Object newInstance(Class klass) {
        try {
            return klass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Results the supplied class and returns a new instance. */
    public static Object newInstance(String klassname) {
        return newInstance(classForName(klassname));
    }

    /**
     * Accepts a byte array key and a total number of shards and returns the appropriate shard for
     * the supplied key.
     */
    public static int keyShard(byte[] key, int numShards) {
        BigInteger hash = new BigInteger(md5Hash(key));
        return hash.mod(new BigInteger("" + numShards)).intValue();
    }

    public static String convertStreamToString(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } finally {
            is.close();
        }
        return sb.toString();
    }

    public static void setObject(JobConf conf, String key, Object o) {
        conf.set(key, StringUtils.byteToHexString(serializeObject(o)));
    }

    public static Object getObject(JobConf conf, String key) {
        String s = conf.get(key);
        if (s == null) { return null; }
        byte[] val = StringUtils.hexStringToByte(s);
        return deserializeObject(val);
    }

    public static byte[] serializeObject(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object deserializeObject(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeByteArray(DataOutput out, byte[] arr) throws IOException {
        out.writeInt(arr.length);
        out.write(arr);
    }

    public static byte[] readByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] ret = new byte[length];
        in.readFully(ret);
        return ret;
    }

    public static FileSystem getFS(String path, Configuration c) throws IOException {
        try {
            return FileSystem.get(new URI(path), c);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> getPersistenceOptions(Map<String, Map<String, Object>> opts, Coordinator fact) {
        return opts.get(fact.getClass().getName());
    }

    public static Object get(Map m, Object key, Object defaultVal) {
        return (!m.containsKey(key)) ? defaultVal : m.get(key);
    }

    /**
     * Returns a byte array containing the cargo of the supplied BytesWritable object.
     * @param bw
     * @return
     */
    public static byte[] getBytes(BytesWritable bw) {
        byte[] padded = bw.getBytes();
        byte[] ret = new byte[bw.getLength()];
        System.arraycopy(padded, 0, ret, 0, ret.length);
        return ret;
    }

    public static Iterable<KryoFactory.ClassPair> buildClassPairs(List<List<String>> stringPairs) {

        List<KryoFactory.ClassPair> retPairs = new ArrayList<KryoFactory.ClassPair>();
        for(List<String> pair: stringPairs) {
            Class klass = Utils.classForName(pair.get(0));

            String serializerName = pair.get(1);
            KryoFactory.ClassPair classPair;
            if (serializerName == null)
                classPair = new KryoFactory.ClassPair(klass);
            else
                classPair = new KryoFactory.ClassPair(klass, Utils.classForName(serializerName));

            retPairs.add(classPair);
        }

        return retPairs;
    }

    /**
     * Returns a KryoSerializer object with the same serializations registered as the supplied DomainSpec.
     * @param spec
     * @return
     */
    public static Serializer makeSerializer(DomainSpec spec) {
        List<List<String>> pairs = spec.getKryoPairs();
        return new KryoSerializer(buildClassPairs(pairs));
    }

    /**
     * If the supplied SerializationWrapper has the same kryoPairs, we ignore it. Else we replace the
     * existing serializer with a new one that contains the serialization pairs in the supplied DomainSpec.
     * @param wrapper
     * @param spec
     */
    public static void prepSerializationWrapper(SerializationWrapper wrapper, DomainSpec spec) {
        KryoSerializer buf = (KryoSerializer) wrapper.getSerializer();

        if (buf == null || buf.getKryoPairs() != buildClassPairs(spec.getKryoPairs()))
            wrapper.setSerializer(makeSerializer(spec));
    }
}
