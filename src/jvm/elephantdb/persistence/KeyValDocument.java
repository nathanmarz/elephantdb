package elephantdb.persistence;

/** User: sritchie Date: 12/6/11 Time: 10:30 AM */
public class KeyValDocument<K, V> implements Document {
    public K key;
    public V value;

    public KeyValDocument() {
    }

    public KeyValDocument(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
