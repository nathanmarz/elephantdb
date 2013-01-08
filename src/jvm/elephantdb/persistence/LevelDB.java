package elephantdb.persistence;

import elephantdb.document.KeyValDocument;
import elephantdb.serialize.SerializationWrapper;
import elephantdb.serialize.Serializer;
import org.apache.log4j.Logger;

import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import org.fusesource.leveldbjni.internal.JniDB;

import java.io.File;
import java.io.IOException;

import java.util.Map;

public class LevelDB implements SerializationWrapper, Coordinator {
    public static Logger LOG = Logger.getLogger(LevelDB.class);
    public Serializer serializer;

    public LevelDB() {
        super();
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public Persistence openPersistenceForRead(String root, Map options) throws IOException {
        return new LevelDBPersistence(root, getSerializer(), options, true, false);
    }

    public Persistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new LevelDBPersistence(root, getSerializer(), options, false, false);
    }

    public Persistence createPersistence(String root, Map options) throws IOException {
        LevelDBPersistence ret = new LevelDBPersistence(root, getSerializer(), options, false, true);
        ret.close();
        return ret;
    }

    public static class LevelDBPersistence implements KeyValPersistence {
        Serializer kvSerializer;
        Options dboptions;
        DB db;
        boolean readOnly;
        
        public LevelDBPersistence(String root, Serializer serializer, Map options, 
                                  boolean readOnly, boolean allowCreate) throws IOException {
            if (serializer == null)
                throw new RuntimeException("LevelDBPersistence requires an initialized serializer.");

            this.kvSerializer = serializer;
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

        public <K, V> V get(K key) throws IOException {
            byte[] k = kvSerializer.serialize(key);

            byte[] valBytes = db.get(k);
            if(valBytes != null) {
                return (V) kvSerializer.deserialize(valBytes);
            } else {
                return null;
            }
        }

        public <K, V> void put(K key, V value) throws IOException {
            index(new KeyValDocument<K, V>(key, value));
        }

        private void add(byte[] key, byte[] value) throws IOException {
            db.put(key, value);
        }

        public void index(KeyValDocument document) throws IOException {
            byte[] k = kvSerializer.serialize(document.key);
            byte[] v = kvSerializer.serialize(document.value);
            add(k, v);
        }

        public void close() throws IOException {
            if(!readOnly) {
                LOG.info("Compacting leveldb");
                String stats = db.getProperty("leveldb.stats");
                LOG.info(stats);
                ((JniDB) db).compactRange(null,null);
                LOG.info("Done compacting leveldb");
                stats = db.getProperty("leveldb.stats");
                LOG.info(stats);
            }

            db.close();
        }

        public CloseableIterator<KeyValDocument> iterator() {
            return new CloseableIterator<KeyValDocument>() {
                DBIterator cursor = null;
                KeyValDocument next = null;
                
                private void cacheNext() {
                    if (cursor.hasNext()) {
                        Object k = kvSerializer.deserialize(cursor.peekNext().getKey());
                        Object v = kvSerializer.deserialize(cursor.peekNext().getValue());
                    
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
