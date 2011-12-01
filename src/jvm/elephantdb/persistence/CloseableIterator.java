package elephantdb.persistence;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Need this for iterating through all keys and values on a specific shard.
 * @param <D>
 */

public interface CloseableIterator<D> extends Iterator<D>, Closeable {

}
