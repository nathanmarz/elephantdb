package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.LocalPersistence;
import elephantdb.persistence.LocalPersistenceFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;


public class ElephantOutputFormat implements OutputFormat<IntWritable, ElephantRecordWritable> {
    public static Logger LOG = Logger.getLogger(ElephantOutputFormat.class);

    public static final String ARGS_CONF = "elephant.output.args";

    public static class Args implements Serializable {
        public DomainSpec spec;
        public String outputDirHdfs;

        public ElephantUpdater updater = new ReplaceUpdater();
        public String updateDirHdfs = null;
        public Map<String, Object> persistenceOptions = new HashMap<String, Object>();

        public Args(DomainSpec spec, String outputDirHdfs) {
            this.spec = spec;
            this.outputDirHdfs = outputDirHdfs;
        }
    }

    public class ElephantRecordWriter implements RecordWriter<IntWritable, ElephantRecordWritable> {

        FileSystem _fs;
        Args _args;
        Map<Integer, LocalPersistence> _lps = new HashMap<Integer, LocalPersistence>();
        Progressable _progressable;
        LocalElephantManager _localManager;

        int _numWritten = 0;
        long _lastCheckpoint = System.currentTimeMillis();

        public ElephantRecordWriter(Configuration conf, Args args, Progressable progressable) throws IOException {
            _fs = Utils.getFS(args.outputDirHdfs, conf);
            _args = args;
            _progressable = progressable;
            _localManager = new LocalElephantManager(_fs, args.spec, args.persistenceOptions, LocalElephantManager.getTmpDirs(conf));
        }

        private String remoteUpdateDirForShard(int shard) {
            if(_args.updateDirHdfs==null) return null;
            else return _args.updateDirHdfs + "/" + shard;
        }

        public void write(IntWritable shard, ElephantRecordWritable record) throws IOException {
            LocalPersistence lp = null;
            LocalPersistenceFactory fact = _args.spec.getLPFactory();
            Map<String, Object> options = _args.persistenceOptions;
            if(_lps.containsKey(shard.get())) {
                lp = _lps.get(shard.get());
            } else {
                String updateDir = remoteUpdateDirForShard(shard.get());
                String localShard = _localManager.downloadRemoteShard("" + shard.get(), updateDir);
                lp = fact.openPersistenceForAppend(localShard, options);
                _lps.put(shard.get(), lp);
                progress();
            }

            _args.updater.updateElephant(lp, record.key, record.val);

            _numWritten++;
            if(_numWritten % 25000 == 0) {
                long now = System.currentTimeMillis();
                long delta = now - _lastCheckpoint;
                _lastCheckpoint = now;
                LOG.info("Wrote last 25000 records in " + delta + " ms");
                _localManager.progress();
            }
        }

        public void close(Reporter reporter) throws IOException {
            for(Integer shard: _lps.keySet()) {
                String lpDir = _localManager.localTmpDir("" + shard);
                LOG.info("Closing LP for shard " + shard + " at " + lpDir);
                _lps.get(shard).close();
                LOG.info("Closed LP for shard " + shard + " at " + lpDir);
                progress();
                String remoteDir = _args.outputDirHdfs + "/" + shard;
                if(_fs.exists(new Path(remoteDir))) {
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
            if(_progressable!=null) _progressable.progress();
        }
    }

    public RecordWriter<IntWritable, ElephantRecordWritable> getRecordWriter(FileSystem fs, JobConf conf, String string, Progressable progressable) throws IOException {
        return new ElephantRecordWriter(conf, (Args) Utils.getObject(conf, ARGS_CONF), progressable);
    }

    public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
        Args args = (Args) Utils.getObject(conf, ARGS_CONF);
        fs = Utils.getFS(args.outputDirHdfs, conf);
        if(conf.getBoolean("mapred.reduce.tasks.speculative.execution", true)) {
            throw new InvalidJobConfException("Speculative execution should be false");
        }
        if(fs.exists(new Path(args.outputDirHdfs))) {
            throw new InvalidJobConfException("Output dir already exists " + args.outputDirHdfs);
        }
        if(args.updateDirHdfs!=null && !fs.exists(new Path(args.updateDirHdfs))) {
            throw new InvalidJobConfException("Shards to update does not exist " + args.updateDirHdfs);
        }
    }
}
