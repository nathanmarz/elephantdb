package elephantdb.persistence;

import java.io.IOException;

public interface LocalPersistence extends Iterable {
    public static class KeyValuePair {
        public byte[] key;
        public byte[] value;
        
        public KeyValuePair(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    public byte[] get(byte[] key) throws IOException;
    public void add(byte[] key, byte[] value) throws IOException;
    public void close() throws IOException;
    public CloseableIterator<KeyValuePair> iterator();
}
