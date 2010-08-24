package elephantdb.persistence;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public abstract class LocalPersistenceFactory implements Serializable {
    public abstract LocalPersistence openPersistenceForRead(String root, Map options) throws IOException;
    public abstract LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException;
    public abstract LocalPersistence createPersistence(String root, Map options) throws IOException;
    
    public KeySorter getKeySorter() {
        return new IdentityKeySorter();
    }
}