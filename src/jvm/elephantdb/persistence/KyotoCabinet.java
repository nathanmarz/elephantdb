package elephantdb.persistence;

import java.io.IOException;
import java.util.Map;
import org.apache.log4j.Logger;

/** User: sritchie Date: 11/20/11 Time: 8:20 PM
 *  Shell for KyotoCabinet persistence.
 *  This might help: http://maven.cloudhopper.com/repos/third-party/kyotocabinet/kyotocabinet/1.21/
 * */

public class KyotoCabinet extends LocalPersistenceFactory {
    public static Logger LOG = Logger.getLogger(KyotoCabinet.class);

    @Override public LocalPersistence openPersistenceForRead(String root, Map options) throws IOException {
        return null;
    }

    @Override public LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException {
        return null;
    }

    @Override public LocalPersistence createPersistence(String root, Map options) throws IOException {
        return null;
    }

    public static class KyotoCabinetPersistence implements LocalPersistence {
        public byte[] get(byte[] key) throws IOException {
            return new byte[0];
        }

        public void add(byte[] key, byte[] value) throws IOException {
        }

        public void close() throws IOException {
        }

        public CloseableIterator<KeyValuePair> iterator() {
            return null;
        }
    }
}
