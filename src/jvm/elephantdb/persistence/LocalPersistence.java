package elephantdb.persistence;

import java.io.IOException;

/**
 * LocalPersistence accepts a Document type; the interface allows you to add
 * records to the persistence
 * @param <D>
 */
public interface LocalPersistence<D> extends Iterable {

    // Perhaps the interface here needs to be more flexible about key representation.
    // TODO: Remove GET from this interface.
    public byte[] get(byte[] key) throws IOException;
    public void add(byte[] key, byte[] value) throws IOException;

    // Closing the iterator is different than closing the actual persistence.
    public CloseableIterator<D> iterator();
    public void close() throws IOException;
}
