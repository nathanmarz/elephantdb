package elephantdb.document;

public class KeyValDocument {
    public byte[] key;
    public byte[] value;

    public KeyValDocument() {
    }

    public KeyValDocument(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
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
