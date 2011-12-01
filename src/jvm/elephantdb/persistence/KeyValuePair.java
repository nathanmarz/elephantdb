package elephantdb.persistence;


/**
 * Everything boils down to key-value pairs. The persistence itself is responsible for converting
 * the kv pairs into a suitable representation.
 *
 * This interface is good for k-v databases; set can use empty byte arrays as values, for example.
 * For something like a Lucene document, we might need a richer interface.
 *
 */

public class KeyValuePair {
    public byte[] key;
    public byte[] value;

    public KeyValuePair(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }
}
