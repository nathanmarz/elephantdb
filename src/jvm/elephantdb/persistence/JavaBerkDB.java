package elephantdb.persistence;

import com.sleepycat.je.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class JavaBerkDB extends PersistenceCoordinator {
    public static Logger LOG = Logger.getLogger(File.class);

    public JavaBerkDB() {
        super();
    }

    @Override
    public LocalPersistence openPersistenceForRead(String root, Map options) throws IOException {
        return new JavaBerkDBPersistence(root, getKryoBuffer(), options, true);
    }

    @Override
    public LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new JavaBerkDBPersistence(root, getKryoBuffer(), options, false);
    }

    @Override
    public LocalPersistence createPersistence(String root, Map options) throws IOException {
        return new JavaBerkDBPersistence(root, getKryoBuffer(), options, false);
    }

    public static class JavaBerkDBPersistence implements LocalPersistence<KeyValDocument> {
        private static final String DATABASE_NAME = "elephant";
        Environment _env;
        Database _db;
        KryoBuffer _kryoBuf;

        public JavaBerkDBPersistence(String root, KryoBuffer kryoBuffer, Map options, boolean readOnly) {
            _kryoBuf = kryoBuffer;

            new File(root).mkdirs();
            EnvironmentConfig envConf = new EnvironmentConfig();
            envConf.setAllowCreate(true);
            envConf.setReadOnly(readOnly);
            envConf.setLocking(false);
            envConf.setTransactional(false);

            // TODO: Loop through options, setConfigParam for each one.
            envConf.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "80");
            envConf.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");

            _env = new Environment(new File(root), envConf);

            DatabaseConfig dbConf = new DatabaseConfig();
            dbConf.setAllowCreate(true);
            dbConf.setReadOnly(readOnly);
            dbConf.setDeferredWrite(true);

            _db = _env.openDatabase(null, DATABASE_NAME, dbConf);
        }

        public <K, V> V get(K key) throws IOException {
            DatabaseEntry chrysalis = new DatabaseEntry();
            byte[] k = _kryoBuf.serialize(key);

            OperationStatus stat = _db.get(null, new DatabaseEntry(k), chrysalis, LockMode.READ_UNCOMMITTED);
            if (stat == OperationStatus.SUCCESS) {
                byte[] valBytes = chrysalis.getData();
                return (V) _kryoBuf.deserialize(valBytes);
            } else {
                return null;
            }
        }

        private void add(byte[] key, byte[] value) throws IOException {
            _db.put(null, new DatabaseEntry(key), new DatabaseEntry(value));
        }

        public void index(KeyValDocument document) throws IOException {
            byte[] k = _kryoBuf.serialize(document.key);
            byte[] v = _kryoBuf.serialize(document.value);
            add(k, v);
        }

        public void close() throws IOException {
            if (!_db.getConfig().getReadOnly()) {
                LOG.info("Syncing environment at " + _env.getHome().getPath());
                _env.sync();
                LOG.info("Done syncing environment at " + _env.getHome().getPath());

                LOG.info("Cleaning environment log at " + _env.getHome().getPath());
                boolean anyCleaned = false;
                while (_env.cleanLog() > 0) {
                    anyCleaned = true;
                }
                LOG.info("Done cleaning environment log at " + _env.getHome().getPath());
                if (anyCleaned) {
                    LOG.info("Checkpointing environment at " + _env.getHome().getPath());
                    CheckpointConfig checkpoint = new CheckpointConfig();
                    checkpoint.setForce(true);
                    _env.checkpoint(checkpoint);
                    LOG.info("Done checkpointing environment at " + _env.getHome().getPath());
                }
            }

            _db.close();
            _env.close();
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
                        Object k = _kryoBuf.deserialize(key.getData());
                        Object v = _kryoBuf.deserialize(val.getData());

                        next = new KeyValDocument(k, v);
                    } else {
                        next = null;
                        close();
                    }
                }

                private void initCursor() {
                    if (cursor == null) {
                        cursor = _db.openCursor(null, null);
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
                    if (cursor != null) { cursor.close(); }
                }

            };
        }

    }

}
