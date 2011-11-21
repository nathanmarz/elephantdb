package elephantdb.persistence;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Need this for iterating through all keys and values on a specific shard.
 * @param <T>
 */

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

}
