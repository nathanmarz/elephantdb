package elephantdb.persistence;

import com.sleepycat.je.*;
import elephantdb.document.KeyValDocument;
import elephantdb.Utils;
import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SnappyJavaBerkDB implements Coordinator {
    public static Logger LOG = Logger.getLogger(SnappyJavaBerkDB.class);

    public SnappyJavaBerkDB() {
        super();
    }

    public Persistence openPersistenceForRead(String root, Map options) throws IOException {
        return new SnappyJavaBerkDBPersistence(root, options, true, false);
    }

    public Persistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new SnappyJavaBerkDBPersistence(root, options, false, false);
    }

    public Persistence createPersistence(String root, Map options) throws IOException {
        SnappyJavaBerkDBPersistence ret = new SnappyJavaBerkDBPersistence(root, options, false, true);
        ret.close();
        return ret;
    }

    public static class SnappyJavaBerkDBPersistence implements KeyValPersistence {
        private static final String DATABASE_NAME = "elephant";
        Environment env;
        Database db;

        public SnappyJavaBerkDBPersistence(String root, Map options,
                                     boolean readOnly, boolean allowCreate) {

            new File(root).mkdirs();
            EnvironmentConfig envConf = new EnvironmentConfig();
            envConf.setAllowCreate(allowCreate);

            envConf.setReadOnly(readOnly);
            envConf.setLocking(false);
            envConf.setTransactional(false);
            envConf.setSharedCache(true);

            // TODO: Loop through options, setConfigParam for each one.
            envConf.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "10");
            envConf.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "5");
            envConf.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
            envConf.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "104857600"); // 100 MB

            env = new Environment(new File(root), envConf);
            
            DatabaseConfig dbConf = new DatabaseConfig();
            dbConf.setAllowCreate(allowCreate);
            dbConf.setReadOnly(readOnly);
            dbConf.setDeferredWrite(true);
            
            dbConf.setNodeMaxEntries(512);

            db = env.openDatabase(null, DATABASE_NAME, dbConf);
        }

        public byte[] get(byte[] key) throws IOException {
            EnvironmentConfig envConf = env.getConfig();
            
            DatabaseEntry chrysalis = new DatabaseEntry();

            byte[] compressedKey = Snappy.compress(key);
            OperationStatus stat = db.get(null, new DatabaseEntry(compressedKey), chrysalis, LockMode.READ_UNCOMMITTED);
            if (stat == OperationStatus.SUCCESS) {
                return Snappy.uncompress(chrysalis.getData());
            } else {
                LOG.debug("Lookup failed in " + env.getHome() + ": " + stat);
                return null;
            }
        }

        public void put(byte[] key, byte[] value) throws IOException {
            index(new KeyValDocument(key, value));
        }

        private void add(byte[] key, byte[] value) throws IOException {
            byte[] compressedKey = Snappy.compress(key);
            byte[] compressedValue = Snappy.compress(value);
            db.put(null, new DatabaseEntry(compressedKey), new DatabaseEntry(compressedValue));
        }

        public void index(KeyValDocument document) throws IOException {
            add(document.key, document.value);
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

                private void cacheNext() throws IOException {
                    DatabaseEntry key = new DatabaseEntry();
                    DatabaseEntry val = new DatabaseEntry();

                    // cursor stores the next key and value in the above mutable objects.
                    OperationStatus stat = cursor.getNext(key, val, LockMode.READ_UNCOMMITTED);
                    if (stat == OperationStatus.SUCCESS) {
                        next = new KeyValDocument(Snappy.uncompress(key.getData()), Snappy.uncompress(val.getData()));
                    } else {
                        next = null;
                        close();
                    }
                }

                private void initCursor() {
                    if (cursor == null) {
                        cursor = db.openCursor(null, null);
                        try {
                            cacheNext();
                        } catch (IOException e) {
                            throw new RuntimeException("Unable to cache next key/value pair: " + e);
                        }
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
                    try {
                        cacheNext(); // caches up n + 1,
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to cache next key/value pair: " + e);
                    }
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
