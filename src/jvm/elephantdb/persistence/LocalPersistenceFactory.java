package elephantdb.persistence;

import java.util.Map;

public abstract class LocalPersistenceFactory {
    public abstract LocalPersistence openPersistence(String root, Map options);
    public abstract LocalPersistence createPersistence(String root, Map options);
    
    public KeySorter getKeySorter() {
        return new IdentityKeySorter();
    }
}