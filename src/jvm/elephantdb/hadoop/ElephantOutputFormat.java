package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ElephantOutputFormat implements OutputFormat<IntWritable, BytesWritable> {
    public static Logger LOG = Logger.getLogger(ElephantOutputFormat.class);

    public static final String ARGS_CONF = "elephant.output.args";

    // This gets serialized in via the conf.
    public static class Args implements Serializable {
        public DomainSpec spec;

        // Path to a version inside of a versioned store, perhaps?
        public String outputDirHdfs;

        // Implements the ElephantUpdater interface.
        public ElephantUpdater updater = new IdentityUpdater();

        // If this is set, the output format will go download it!
        public String updateDirHdfs = null;

        // ends up going to PersistenceCoordinator, which passes it on.
        public Map<String, Object> persistenceOptions = new HashMap<String, Object>();

        public Args(DomainSpec spec, String outputDirHdfs) {
            this.spec = spec;
            this.outputDirHdfs = outputDirHdfs;
        }
    }

    public class ElephantRecordWriter implements RecordWriter<IntWritable, BytesWritable> {

        FileSystem _fs;
        Args _args;
        Map<Integer, LocalPersistence> _lps = new HashMap<Integer, LocalPersistence>();
        Progressable _progressable;
        LocalElephantManager _localManager;
        KryoWrapper.KryoBuffer _kryoBuf;

        int _numWritten = 0;
        long _lastCheckpoint = System.currentTimeMillis();

        public ElephantRecordWriter(Configuration conf, Args args, Progressable progressable)
            throws IOException {
            _fs = Utils.getFS(args.outputDirHdfs, conf);
            _args = args;
            _kryoBuf = _args.spec.getCoordinator().getKryoBuffer();
            _progressable = progressable;
            _localManager = new LocalElephantManager(_fs, args.spec, args.persistenceOptions,
                LocalElephantManager.getTmpDirs(conf));
        }

        private String remoteUpdateDirForShard(int shard) {
            if (_args.updateDirHdfs == null) { return null; } else {
                return _args.updateDirHdfs + "/" + shard;
            }
        }
        
        private LocalPersistence retrieveShard(int shardIdx) throws IOException {
            LocalPersistence lp = null;
            PersistenceCoordinator fact = _args.spec.getCoordinator();
            Map<String, Object> options = _args.persistenceOptions;
            if (_lps.containsKey(shardIdx)) {
                lp = _lps.get(shardIdx);
            } else {
                String updateDir = remoteUpdateDirForShard(shardIdx);
                String localShard = _localManager.downloadRemoteShard("" + shardIdx, updateDir);
                lp = fact.openPersistenceForAppend(localShard, options);
                _lps.put(shardIdx, lp);
                progress();
            }
            return lp;
        }

        public void write(IntWritable shard, BytesWritable carrier) throws IOException {
            LocalPersistence lp = retrieveShard(shard.get());

            // TODO: Change this behavior and get Cascading to serialize object.
            Document doc = (Document) _kryoBuf.deserialize(Utils.getBytes(carrier));

            if (_args.updater != null) {
                _args.updater.update(lp, doc);
            } else {
                lp.index(doc);
            }

            bumpProgress();
        }

        public void bumpProgress() {
            _numWritten++;
            if (_numWritten % 25000 == 0) {
                long now = System.currentTimeMillis();
                long delta = now - _lastCheckpoint;
                _lastCheckpoint = now;
                LOG.info("Wrote last 25000 records in " + delta + " ms");
                _localManager.progress();
            }
        }

        public void close(Reporter reporter) throws IOException {
            for (Integer shard : _lps.keySet()) {
                String lpDir = _localManager.localTmpDir("" + shard);
                LOG.info("Closing LP for shard " + shard + " at " + lpDir);
                _lps.get(shard).close();
                LOG.info("Closed LP for shard " + shard + " at " + lpDir);
                progress();
                String remoteDir = _args.outputDirHdfs + "/" + shard;
                if (_fs.exists(new Path(remoteDir))) {
                    LOG.info("Deleting existing shard " + shard + " at " + remoteDir);
                    _fs.delete(new Path(remoteDir), true);
                    LOG.info("Deleted existing shard " + shard + " at " + remoteDir);
                }
                LOG.info("Copying " + lpDir + " to " + remoteDir);
                _fs.copyFromLocalFile(new Path(lpDir), new Path(remoteDir));
                LOG.info("Copied " + lpDir + " to " + remoteDir);
                progress();
            }
            _localManager.cleanup();
        }

        private void progress() {
            if (_progressable != null) { _progressable.progress(); }
        }
    }

    public RecordWriter<IntWritable, BytesWritable> getRecordWriter(FileSystem fs,
        JobConf conf, String string, Progressable progressable) throws IOException {
        return new ElephantRecordWriter(conf, (Args) Utils.getObject(conf, ARGS_CONF), progressable);
    }

    public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
        Args args = (Args) Utils.getObject(conf, ARGS_CONF);
        fs = Utils.getFS(args.outputDirHdfs, conf);
        if (conf.getBoolean("mapred.reduce.tasks.speculative.execution", true)) {
            // Because we don't want to write a bunch of extra times.
            throw new InvalidJobConfException("Speculative execution should be false");
        }
        if (fs.exists(new Path(args.outputDirHdfs))) {
            throw new InvalidJobConfException("Output dir already exists " + args.outputDirHdfs);
        }
        if (args.updateDirHdfs != null && !fs.exists(new Path(args.updateDirHdfs))) {
            throw new InvalidJobConfException(
                "Shards to update does not exist " + args.updateDirHdfs);
        }
    }
}
