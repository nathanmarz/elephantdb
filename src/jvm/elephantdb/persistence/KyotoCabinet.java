package elephantdb.persistence;

import elephantdb.document.KeyValDocument;
import elephantdb.serialize.SerializationWrapper;
import elephantdb.serialize.Serializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/** User: sritchie Date: 11/20/11 Time: 8:20 PM
 *  Shell for KyotoCabinet persistence.
 *  This might help: http://maven.cloudhopper.com/repos/third-party/kyotocabinet/kyotocabinet/1.21/
 * */

public class KyotoCabinet implements SerializationWrapper, Coordinator {
    public static Logger LOG = Logger.getLogger(KyotoCabinet.class);
    Serializer serializer;

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

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public static class KyotoCabinetPersistence implements Persistence<KeyValDocument> {
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
