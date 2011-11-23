package elephantdb.persistence;

/** User: sritchie Date: 11/22/11 Time: 4:01 PM
 *
 * The transmitter deals with serialization and deserialization of keys and values.
 * After serialization, keys and vals are passed through the hadoop machinery, where they
 * reappear as calls to the ElephantUpdater.
 *
 * For search, the updater will most likely only be adding documents to a given shard. For
 * key-value, the updater might need to perform a get and merge values.
 * */
public interface Transmitter {
    public byte[] serializeKey(Object key);
    public byte[] serializeVal(Object val);

    // Needs more options, perhaps, to know how to deserialize.
    public Object deserializeKey(byte[] key);
    public Object deserializeVal(byte[] val);
}
