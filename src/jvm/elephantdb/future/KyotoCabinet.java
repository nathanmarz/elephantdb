package elephantdb.future;

import java.io.IOException;
import java.util.Map;

import elephantdb.persistence.CloseableIterator;
import elephantdb.persistence.KeyValDocument;
import elephantdb.persistence.LocalPersistence;
import elephantdb.persistence.PersistenceCoordinator;
import org.apache.log4j.Logger;

/** User: sritchie Date: 11/20/11 Time: 8:20 PM
 *  Shell for KyotoCabinet persistence.
 *  This might help: http://maven.cloudhopper.com/repos/third-party/kyotocabinet/kyotocabinet/1.21/
 * */

public class KyotoCabinet extends PersistenceCoordinator {
    public static Logger LOG = Logger.getLogger(KyotoCabinet.class);

    public KyotoCabinet() {super();}

    @Override public LocalPersistence openPersistenceForRead(String root, Map options) throws IOException {
        return null;
    }

    @Override public LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException {
        return null;
    }

    @Override public LocalPersistence createPersistence(String root, Map options) throws IOException {
        return null;
    }

    public static class KyotoCabinetPersistence implements LocalPersistence<KeyValDocument> {
        public byte[] get(byte[] key) throws IOException {
            return new byte[0];
        }

        public void index(KeyValDocument document) throws IOException {
        }

        public void close() throws IOException {
        }

        public CloseableIterator<KeyValDocument> iterator() {
            return null;
        }
    }
}
