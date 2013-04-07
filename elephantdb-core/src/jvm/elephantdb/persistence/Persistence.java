package elephantdb.persistence;

import java.io.IOException;

public interface Persistence<D> extends Iterable {

    public void index(D document) throws IOException;

    // Note: closing the iterator is different than closing the actual persistence.
    public CloseableIterator<D> iterator();
    public void close() throws IOException;
}
