package elephantdb.persistence;

public interface LocalPersistence {
    public byte[] get(byte[] key);
    public void add(byte[] key, byte[] value);
    public void close();
}
