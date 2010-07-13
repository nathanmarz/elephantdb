package elephantdb.persistence;

public interface KeySorter {
    public byte[] getSortableKey(byte[] key);
}