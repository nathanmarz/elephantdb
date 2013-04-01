package elephantdb.document;

import java.util.Arrays;

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
        return (Arrays.equals(this.key, other.key) && Arrays.equals(this.value, other.value));
    }
}
