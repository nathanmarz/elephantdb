package elephantdb.persistence;

import java.io.IOException;

public interface LocalPersistence extends Iterable {

    /**
     * Everything boils down to key-value pairs. The persistence itself is responsible for converting
     * the kv pairs into a suitable representation.
     *
     * This interface is good for k-v databases; set can use empty byte arrays as values, for example.
     * For something like a Lucene document, we might need a richer interface.
     *
     */
    public static class KeyValuePair {
        public byte[] key;
        public byte[] value;

        public KeyValuePair(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    // Perhaps the interface here needs to be more flexible about key representation.
    public byte[] get(byte[] key) throws IOException;
    public void add(byte[] key, byte[] value) throws IOException;
    public void close() throws IOException;

    // Should this actually just return objects?
    public CloseableIterator<KeyValuePair> iterator();
}
