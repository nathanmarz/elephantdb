package elephantdb.persistence;

import javax.sound.midi.Transmitter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * "Shard Creators" inherit from this class.
 * On the client side, openPersistenceForRead is called like this:
 *
 *  (.getLPFactory (DomainSpec/readFromFileSystem fs path))
 *
 *  .getLPFactory returns a new instance of the class inside that string. SO! This   
 */
public abstract class LocalPersistenceFactory implements Serializable {
    public abstract LocalPersistence openPersistenceForRead(String root, Map options) throws IOException;
    public abstract LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException;
    public abstract LocalPersistence createPersistence(String root, Map options) throws IOException;

    public abstract elephantdb.persistence.Transmitter getTransmitter();
    public abstract Sharder getSharder();

    // what's the deal here?
    public KeySorter getKeySorter() {
        return new IdentityKeySorter();
    }

    // Need methods for key and value serialization here, or some way of specifying how to do this.
    // Something that interfaces a serialization interface.
}
