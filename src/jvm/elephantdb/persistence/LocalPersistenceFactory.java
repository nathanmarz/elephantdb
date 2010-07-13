package elephantdb.persistence;

import java.util.Map;

public abstract class LocalPersistenceFactory {
    public abstract LocalPersistence getPersistence(String root, Map options);
    
    public KeySorter getKeySorter() {
        return new IdentityKeySorter();
    }
}