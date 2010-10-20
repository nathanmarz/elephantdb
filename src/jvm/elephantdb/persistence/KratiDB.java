package elephantdb.persistence;

import elephantdb.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import krati.cds.impl.segment.MappedSegmentFactory;
import krati.cds.impl.store.DynamicDataStore;
import krati.util.FnvHashFunction;
import org.apache.log4j.Logger;


public class KratiDB extends LocalPersistenceFactory {
    public static Logger LOG = Logger.getLogger(KratiDB.class);
    public static final String INIT_LEVEL = "init-level";
    public static final String SEGMENT_SIZE_MB = "segment-size-mb";
    public static final String LOAD_FACTOR = "load-factor";


    @Override
    public LocalPersistence openPersistenceForRead(String root, Map options) throws IOException {
        return new KratiPersistence(root, options);
    }

    @Override
    public LocalPersistence openPersistenceForAppend(String root, Map options) throws IOException {
        return new KratiPersistence(root, options);
    }

    @Override
    public LocalPersistence createPersistence(String root, Map options) throws IOException {
        return new KratiPersistence(root, options);
    }

    public static class KratiPersistence implements LocalPersistence {
        private final DynamicDataStore _datastore;

        public KratiPersistence(String root, Map<String, Object> options) throws IOException {
            try {
                LOG.info("Opening Krati at " + root);
                _datastore = new DynamicDataStore(
                        new File(root),
                        (Integer) Utils.get(options, INIT_LEVEL, 2),
                        (Integer) Utils.get(options, SEGMENT_SIZE_MB, 256),
                        new MappedSegmentFactory(),
                        (Double) Utils.get(options, LOAD_FACTOR, 0.75),
                        new FnvHashFunction());
                LOG.info("Done opening Krati");
            } catch(Exception e) {
                throw new IOException(e);
            }
        }

        public byte[] get(byte[] key) throws IOException {
            return _datastore.get(key);
        }

        public void add(byte[] key, byte[] value) throws IOException {
            try {
                _datastore.put(key, value);
            } catch(Exception e) {
                throw new IOException(e);
            }
        }

        public void close() throws IOException {
            LOG.info("Persisting Krati");
            _datastore.persist();
            LOG.info("Syncing Krati");
            _datastore.sync();
            LOG.info("Done closing Krati");
        }

        public CloseableIterator<KeyValuePair> iterator() {
            //TODO: finish
            throw new UnsupportedOperationException("Not supported yet.");
        }
      

    }
}
