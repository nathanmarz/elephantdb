package elephantdb.persistence;

public class IdentityKeySorter implements KeySorter {
    public byte[] getSortableKey(byte[] key) {
        return key;
    }
}