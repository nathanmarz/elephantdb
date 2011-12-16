package elephantdb.persistence;

import java.io.IOException;

/**
 * Persistence accepts a Document type; the interface allows you to add
 * records to the persistence.
 * @param <D>
 */
public interface Persistence<D extends Document> extends Iterable {

    public void index(D document) throws IOException;

    // Note: closing the iterator is different than closing the actual persistence.
    public CloseableIterator<D> iterator();
    public void close() throws IOException;
}
