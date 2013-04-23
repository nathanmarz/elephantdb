package elephantdb.persistence;

import elephantdb.document.KeyValDocument;
import org.apache.log4j.Logger;

import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import org.fusesource.leveldbjni.internal.JniDB;

import java.io.File;
import java.io.IOException;

import java.util.Map;

public class LevelDB implements Coordinator {
    public static Logger LOG = Logger.getLogger(LevelDB.class);

    public LevelDB() {
        super();
    }

    public Persistence openPersistenceForRead(String root, Map options) throws IOException {
        return new LevelDBPersistence(root, options, true, false);
    }

    public Persistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new LevelDBPersistence(root, options, false, false);
    }

    public Persistence createPersistence(String root, Map options) throws IOException {
        LevelDBPersistence ret = new LevelDBPersistence(root, options, false, true);
        ret.close();
        return ret;
    }

    public static class LevelDBPersistence implements KeyValPersistence {
        Options dboptions;
        DB db;
        boolean readOnly;
        
        public LevelDBPersistence(String root, Map options, 
                                  boolean readOnly, boolean allowCreate) throws IOException {
            this.readOnly = readOnly;
            new File(root).mkdirs();

            dboptions = new Options();
            dboptions.createIfMissing(allowCreate);
            dboptions.paranoidChecks(true);
            // 20mb TODO: set this using the options map
            dboptions.cacheSize(20 * 1024 * 1024);
            dboptions.compressionType(CompressionType.SNAPPY);

            db = factory.open(new File(root), dboptions);

        }

        public byte[] get(byte[] key) throws IOException {
            return db.get(key);
        }

        public void put(byte[] key, byte[] value) throws IOException {
            index(new KeyValDocument(key, value));
        }

        private void add(byte[] key, byte[] value) throws IOException {
            db.put(key, value);
        }

        public void index(KeyValDocument document) throws IOException {
            add(document.key, document.value);
        }

        public void close() throws IOException {
            db.close();
        }

        public CloseableIterator<KeyValDocument> iterator() {
            return new CloseableIterator<KeyValDocument>() {
                DBIterator cursor = null;
                KeyValDocument next = null;
                
                private void cacheNext() {
                    if (cursor.hasNext()) {
                        byte[] k = cursor.peekNext().getKey();
                        byte[] v = cursor.peekNext().getValue();
                    
                        next = new KeyValDocument(k, v);
                        cursor.next();
                    } else {
                        next = null;
                        close();
                    }
                }

                private void initCursor() {
                    if (cursor == null) {
                        cursor = db.iterator();
                        cursor.seekToFirst();
                        cacheNext();
                    }
                }

                public boolean hasNext() {
                    initCursor();
                    return next != null;
                }

                public KeyValDocument next() {
                    initCursor();
                    if (next == null) { throw new RuntimeException("No key/value pair available"); }
                    KeyValDocument ret = next;
                    cacheNext();
                    return ret;
                }

                public void remove() {
                    throw new UnsupportedOperationException("Not supported.");
                }

                public void close() {
                    if (cursor != null)
                        try {
                            cursor.close();
                        } catch (IOException e) {
                            throw new RuntimeException("Unable to close iterator: " + e);
                        }
                }
            };
        }
    }
}
