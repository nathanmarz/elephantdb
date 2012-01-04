package elephantdb.persistence;

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
public class Lucene implements Coordinator {

    public static Logger LOG = Logger.getLogger(File.class);

    public Lucene() {
        super();
    }

    public Persistence openPersistenceForRead(String root, Map options) throws IOException {
        return new LucenePersistence(root, options);
    }

    public Persistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new LucenePersistence(root, options);
    }

    public Persistence createPersistence(String root, Map options) throws IOException {
        return new LucenePersistence(root, options);
    }

    public static class LucenePersistence implements SearchPersistence<Document> {
        Directory rootDir;
        IndexReader reader;

        private static final String INDEX_NAME = "elephant";

        public LucenePersistence(String root, Map options) {
            try {
                NIOFSDirectory dir = new NIOFSDirectory(new File(root));
                reader = IndexReader.open(dir);
                rootDir = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public byte[] get(byte[] key) throws IOException {
            return new byte[0];
        }

        public void close() throws IOException {
            reader.close();
        }

        public void index(Document doc) throws IOException {
            // should be easy to implement. Lucene is going to get LuceneDocuments, and that's IT!
        }

        public CloseableIterator<Document> iterator() {
            return new CloseableIterator<Document>() {
                int idx = 0;
                Integer docCount = null;
                Document nextDoc = null;

                private void cacheNext() {

                    while (idx < docCount && reader.isDeleted(idx)) {
                        idx++;
                    }

                    if (idx + 1 == docCount) {
                        nextDoc = null;
                        close();
                    } else {
                        try {
                            nextDoc = reader.document(idx);
                        } catch (CorruptIndexException ci) {
                            throw new RuntimeException(ci);
                        } catch (IOException io) {
                            throw new RuntimeException(io);
                        }
                    }
                }

                private void initCursor() {
                    if (docCount == null) {
                        docCount = reader.maxDoc();
                        cacheNext();
                    }
                }

                public boolean hasNext() {
                    initCursor();
                    return nextDoc != null;
                }

                public Document next() {
                    initCursor();
                    if (nextDoc == null) {
                        throw new RuntimeException("No document available");
                    }
                    Document ret = nextDoc; // not pointers, so we actually store the value?
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