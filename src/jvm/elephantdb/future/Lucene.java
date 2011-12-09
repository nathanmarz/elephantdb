package elephantdb.future;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.CloseableIterator;
import elephantdb.persistence.KeyValDocument;
import elephantdb.persistence.LocalPersistence;
import elephantdb.persistence.PersistenceCoordinator;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/** User: sritchie Date: 11/21/11 Time: 5:43 PM */
public class Lucene extends PersistenceCoordinator {

    public static Logger LOG = Logger.getLogger(File.class);

    public Lucene() {super();}

    @Override public LocalPersistence openPersistenceForRead(String root, DomainSpec spec, Map options)
        throws IOException {
        return new LucenePersistence(root, options);
    }

    @Override public LocalPersistence openPersistenceForAppend(String root, DomainSpec spec, Map options)
        throws IOException {
        return new LucenePersistence(root, options);
    }

    @Override public LocalPersistence createPersistence(String root, DomainSpec spec, Map options)
        throws IOException {
        return new LucenePersistence(root, options);
    }

    public static class LucenePersistence implements LocalPersistence<KeyValDocument> {
        Directory _rootDir;
        IndexReader _reader;

        private static final String INDEX_NAME = "elephant";

        public LucenePersistence(String root, Map options) {
            try {
                NIOFSDirectory dir = new NIOFSDirectory(new File(root));
                _reader = IndexReader.open(dir);
                _rootDir = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public byte[] get(byte[] key) throws IOException {
            return new byte[0];
        }

        public void close() throws IOException {
            _reader.close();
        }

        public void index(KeyValDocument document) throws IOException {
            // should be easy to implement. Lucene is going to get LuceneDocuments, and that's IT!
        }

        public CloseableIterator<KeyValDocument> iterator() {
            return new CloseableIterator<KeyValDocument>() {
                int idx = 0;
                Integer docCount = null;
                KeyValDocument nextDoc = null;

                private void cacheNext() {

                    while (idx < docCount && _reader.isDeleted(idx)) {
                        idx++;
                    }

                    if (idx + 1 == docCount) {
                        nextDoc = null;
                        close();
                    } else {
                        try {
                            Document doc = _reader.document(idx);
                            byte[] docVal = Utils.serializeObject(doc);
                            byte[] docKey = new byte[0];
                            nextDoc = new KeyValDocument(docKey, docVal);
                        } catch (CorruptIndexException ci) {
                            throw new RuntimeException(ci);
                        } catch (IOException io) {
                            throw new RuntimeException(io);
                        }
                    }
                }

                private void initCursor() {
                    if (docCount == null) {
                        docCount = _reader.maxDoc();
                        cacheNext();
                    }
                }

                public boolean hasNext() {
                    initCursor();
                    return nextDoc != null;
                }

                public KeyValDocument next() {
                    initCursor();
                    if (nextDoc == null) {
                        throw new RuntimeException("No key/value pair available");
                    }
                    KeyValDocument ret = nextDoc; // not pointers, so we actually store the value?
                    cacheNext(); // caches up n + 1,
                    return ret;  // return the old.
                }

                public void remove() {
                    throw new UnsupportedOperationException("Not supported.");
                }

                public void close() {
                }
            };
        }
    }
}