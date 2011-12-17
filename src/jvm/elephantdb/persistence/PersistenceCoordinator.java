package elephantdb.persistence;

import java.io.IOException;
import java.util.Map;

/**
 * "Shard Creators" inherit from this class.
 * On the client side, openPersistenceForRead is called like this:
 *
 *  (.getCoordinator (DomainSpec/readFromFileSystem fs path))
 *
 *  .getCoordinator returns a new instance of the class referenced by that string.
 */
public abstract class PersistenceCoordinator extends KryoWrapper implements Coordinator {

    public abstract Persistence openPersistenceForRead(String root, Map options) throws IOException;
    public abstract Persistence openPersistenceForAppend(String root, Map options) throws IOException;
    public abstract Persistence createPersistence(String root, Map options) throws IOException;

    public KeySorter getKeySorter() {
        return new IdentityKeySorter();
    }
}
