package elephantdb;

import elephantdb.persistence.LocalPersistenceFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

public class Utils {

    public static byte[] md5Hash(byte[] key) {
        try {
            return MessageDigest.getInstance("MD5").digest(key);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static Class classForName(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Object newInstance(Class klass) {
        try {
            return klass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static Object newInstance(String klassname) {
        return newInstance(classForName(klassname));
    }

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

    public static byte[] serializeInt(int i) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(4);
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            dos.writeInt(i);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bos.toByteArray();
    }

    public static byte[] serializeLong(long l) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(4);
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            dos.writeLong(l);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bos.toByteArray();

    }

    public static byte[] serializeString(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void setObject(JobConf conf, String key, Object o) {
        conf.set(key, StringUtils.byteToHexString(serializeObject(o)));
    }

    public static Object getObject(JobConf conf, String key) {
        String s = conf.get(key);
        if(s==null) return null;
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
        } catch(IOException ioe) {
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
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
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
        return new Path(path).getFileSystem(c);
    }

    public static Map<String, Object> getPersistenceOptions(Map<String, Map<String, Object>> opts, LocalPersistenceFactory fact) {
        return opts.get(fact.getClass().getName());
    }

}
