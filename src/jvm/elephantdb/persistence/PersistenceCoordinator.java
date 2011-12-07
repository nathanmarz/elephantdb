package elephantdb.persistence;

import com.esotericsoftware.kryo.ObjectBuffer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * "Shard Creators" inherit from this class.
 * On the client side, openPersistenceForRead is called like this:
 *
 *  (.getCoordinator (DomainSpec/readFromFileSystem fs path))
 *
 *  .getCoordinator returns a new instance of the class referenced by that string.
 */
public abstract class PersistenceCoordinator extends SpecifiedObject implements Serializable {
    public abstract LocalPersistence openPersistenceForRead(String root, Map options) throws IOException;
    public abstract LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException;
    public abstract LocalPersistence createPersistence(String root, Map options) throws IOException;

    public KeySorter getKeySorter() {
        return new IdentityKeySorter();
    }
}
