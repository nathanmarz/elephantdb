package elephantdb.persistence;

import com.sleepycat.je.*;
import elephantdb.document.KeyValDocument;
import elephantdb.serialize.SerializationWrapper;
import elephantdb.serialize.Serializer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class JavaBerkDB implements SerializationWrapper, Coordinator {
    public static Logger LOG = Logger.getLogger(JavaBerkDB.class);
    private Serializer serializer;

    public JavaBerkDB() {
        super();
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public Persistence openPersistenceForRead(String root, Map options) throws IOException {
        return new JavaBerkDBPersistence(root, getSerializer(), options, true, false);
    }

    public Persistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new JavaBerkDBPersistence(root, getSerializer(), options, false, false);
    }

    public Persistence createPersistence(String root, Map options) throws IOException {
        JavaBerkDBPersistence ret = new JavaBerkDBPersistence(root, getSerializer(), options, false, true);
        ret.close();
        return ret;
    }

    public static class JavaBerkDBPersistence implements KeyValPersistence {
        private static final String DATABASE_NAME = "elephant";
        Environment env;
        Database db;
        Serializer kvSerializer;

        public JavaBerkDBPersistence(String root, Serializer serializer, Map options,
                                     boolean readOnly, boolean allowCreate) {
            if (serializer == null)
                throw new RuntimeException("JavaBerkDBPersistence requires an initialized serializer.");

            this.kvSerializer = serializer;

            new File(root).mkdirs();
            EnvironmentConfig envConf = new EnvironmentConfig();
            envConf.setAllowCreate(allowCreate);

            envConf.setReadOnly(readOnly);
            envConf.setLocking(false);
            envConf.setTransactional(false);

            // TODO: Loop through options, setConfigParam for each one.
            envConf.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "80");
            envConf.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");

            env = new Environment(new File(root), envConf);
            
            DatabaseConfig dbConf = new DatabaseConfig();
            dbConf.setAllowCreate(allowCreate);
            dbConf.setReadOnly(readOnly);
            dbConf.setDeferredWrite(true);

            db = env.openDatabase(null, DATABASE_NAME, dbConf);
        }

        public <K, V> V get(K key) throws IOException {
            DatabaseEntry chrysalis = new DatabaseEntry();
            byte[] k = kvSerializer.serialize(key);

            OperationStatus stat = db.get(null, new DatabaseEntry(k), chrysalis, LockMode.READ_UNCOMMITTED);
            if (stat == OperationStatus.SUCCESS) {
                byte[] valBytes = chrysalis.getData();
                LOG.debug("Sending " + valBytes.length + " bytes into deserialize");
                return (V) kvSerializer.deserialize(valBytes);
            } else {
                LOG.debug("Lookup failed: " + stat);
                return null;
            }
        }

        public <K, V> void put(K key, V value) throws IOException {
            index(new KeyValDocument<K, V>(key, value));
        }

        private void add(byte[] key, byte[] value) throws IOException {
            db.put(null, new DatabaseEntry(key), new DatabaseEntry(value));
        }

        public void index(KeyValDocument document) throws IOException {
            byte[] k = kvSerializer.serialize(document.key);
            byte[] v = kvSerializer.serialize(document.value);
            add(k, v);
        }

        public void close() throws IOException {
            if (!db.getConfig().getReadOnly()) {
                LOG.info("Syncing environment at " + env.getHome().getPath());
                env.sync();
                LOG.info("Done syncing environment at " + env.getHome().getPath());

                LOG.info("Cleaning environment log at " + env.getHome().getPath());
                boolean anyCleaned = false;
                while (env.cleanLog() > 0) {
                    anyCleaned = true;
                }
                LOG.info("Done cleaning environment log at " + env.getHome().getPath());
                if (anyCleaned) {
                    LOG.info("Checkpointing environment at " + env.getHome().getPath());
                    CheckpointConfig checkpoint = new CheckpointConfig();
                    checkpoint.setForce(true);
                    env.checkpoint(checkpoint);
                    LOG.info("Done checkpointing environment at " + env.getHome().getPath());
                }
            }

            db.close();
            env.close();
        }

        public CloseableIterator<KeyValDocument> iterator() {
            return new CloseableIterator<KeyValDocument>() {
                Cursor cursor = null;
                KeyValDocument next = null;

                private void cacheNext() {
                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry val = new DatabaseEntry();

                    // cursor stores the next key and value in the above mutable objects.
                    OperationStatus stat = cursor.getNext(key, val, LockMode.READ_UNCOMMITTED);
                    if (stat == OperationStatus.SUCCESS) {
                        Object k = kvSerializer.deserialize(key.getData());
                        Object v = kvSerializer.deserialize(val.getData());

                        next = new KeyValDocument(k, v);
                    } else {
                        next = null;
                        close();
                    }
                }

                private void initCursor() {
                    if (cursor == null) {
                        cursor = db.openCursor(null, null);
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
                    cacheNext(); // caches up n + 1,
                    return ret;  // return the old.
                }

                public void remove() {
                    throw new UnsupportedOperationException("Not supported.");
                }

                public void close() {
                    if (cursor != null)
                        cursor.close();
                }

            };
        }
    }

}
