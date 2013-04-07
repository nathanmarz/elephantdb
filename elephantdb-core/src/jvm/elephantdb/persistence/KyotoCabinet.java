package elephantdb.persistence;

import elephantdb.document.KeyValDocument;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class KyotoCabinet implements Coordinator {
    public static Logger LOG = Logger.getLogger(KyotoCabinet.class);

    public KyotoCabinet() {super();}

    public Persistence openPersistenceForRead(String root, Map options) throws IOException {
        return null;
    }

    public Persistence openPersistenceForAppend(String root, Map options) throws IOException {
        return null;
    }

    public Persistence createPersistence(String root, Map options) throws IOException {
        return null;
    }

    public static class KyotoCabinetPersistence implements KeyValPersistence {
        public byte[] get(byte[] key) throws IOException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public void put(byte[] key, byte[] value) throws IOException {
            index(new KeyValDocument(key, value));
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
