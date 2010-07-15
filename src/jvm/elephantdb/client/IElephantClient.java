package elephantdb.client;

public interface IElephantClient {
    byte[] get(String domain, byte[] key);
    byte[] getString(String domain, String key);
    byte[] getInt(String domain, int key);
    byte[] getLong(String domain, long key);
}