package elephantdb.document;

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

    @Override public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != getClass())
            return false;

        KeyValDocument other = (KeyValDocument) obj;
        return (this.key.equals(other.key) && this.value.equals(other.value));
    }
}
