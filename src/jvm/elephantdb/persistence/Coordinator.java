package elephantdb.persistence;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * User: sritchie
 * Date: 12/16/11
 * Time: 1:37 PM
 */
public interface Coordinator extends Serializable {
    Persistence openPersistenceForRead(String root, Map options) throws IOException;
    Persistence openPersistenceForAppend(String root, Map options) throws IOException;

    /**
     * The returned persistence should be CLOSED.
     * @param root
     * @param options
     * @return
     * @throws IOException
     */
    Persistence createPersistence(String root, Map options) throws IOException;
}
