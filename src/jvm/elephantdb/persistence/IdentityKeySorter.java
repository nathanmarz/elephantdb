package elephantdb.persistence;

/** User: sritchie Date: 12/6/11 Time: 3:06 PM */
public class IdentityKeySorter implements KeySorter {
    public byte[] getSortableKey(byte[] key) {
        return key;
    }
}
