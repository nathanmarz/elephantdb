package elephantdb.persistence;

/** User: sritchie Date: 11/22/11 Time: 4:01 PM */
public interface Transmitter {
    public byte[] serializeKey(Object key);
    public byte[] serializeVal(Object val);

    // Needs more options, perhaps, to know how to deserialize.
    public Object deserializeKey(byte[] key);
    public Object deserializeVal(byte[] val);
}
