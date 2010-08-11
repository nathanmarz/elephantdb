package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.LocalPersistence;
import elephantdb.persistence.LocalPersistenceFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ChecksumFileSystem;
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
        public Map<String, Map<String, Object>> persistenceOptions = new HashMap<String, Map<String, Object>>();
    
        public List<String> tmpDirs = new ArrayList<String>() {{
            add("/tmp");
        }};

        public Args(DomainSpec spec, String outputDirHdfs) {
            this.spec = spec;
            this.outputDirHdfs = outputDirHdfs;
        }
        
        public void setTmpDirs(List<String> dirs) {
            this.tmpDirs = dirs;
        }
    }

    public class ElephantRecordWriter implements RecordWriter<IntWritable, ElephantRecordWritable> {

        FileSystem _fs;
        Args _args;
        File _dirFlag;
        String _localRoot;
        Map<Integer, LocalPersistence> _lps = new HashMap<Integer, LocalPersistence>();
        Progressable _progressable;

        int _numWritten = 0;
        long _lastCheckpoint = System.currentTimeMillis();

        private void clearStaleFlags(List<String> tmpDirs) {
            //delete any flags more than an hour old
            for(String tmp: tmpDirs) {
                File flagDir = new File(flagDir(tmp));
                flagDir.mkdirs();
                for(File flag: flagDir.listFiles()) {
                    if(flag.lastModified() < System.currentTimeMillis() - 1000*60*60) {
                        flag.delete();
                    }
                }
            }
        }

        private String flagDir(String tmpDir) {
            return tmpDir + "/flags";
        }

        private String localPersistenceDir(Integer shard) {
            return _localRoot + "/" + shard;
        }

        private String selectAndFlagRoot(List<String> tmpDirs) throws IOException {
            clearStaleFlags(tmpDirs);
            Map<String, Integer> flagCounts = new HashMap<String, Integer>();
            for(String tmp: tmpDirs) {
                File flagDir = new File(flagDir(tmp));
                flagDir.mkdirs();
                flagCounts.put(tmp, flagDir.list().length);
            }
            String best = null;
            Integer bestCount = null;
            for(Entry<String, Integer> e: flagCounts.entrySet()) {
                if(best==null || e.getValue() < bestCount) {
                    best = e.getKey();
                    bestCount = e.getValue();
                }
            }
            String token = UUID.randomUUID().toString();
            _dirFlag = new File(flagDir(best) + "/" + token);
            _dirFlag.createNewFile();
            return best + "/" + token;
        }


        public ElephantRecordWriter(Configuration conf, Args args, Progressable progressable) throws IOException {
            _fs = Utils.getFS(args.outputDirHdfs, conf);
            _args = args;
            _localRoot = selectAndFlagRoot(args.tmpDirs);
            _progressable = progressable;

        }

        public void write(IntWritable shard, ElephantRecordWritable record) throws IOException {
            LocalPersistence lp = null;
            LocalPersistenceFactory fact = _args.spec.getLPFactory();
            Map<String, Object> options = Utils.getPersistenceOptions(_args.persistenceOptions, fact);
            if(_lps.containsKey(shard.get())) {
                lp = _lps.get(shard.get());
            } else {
                String localLPDir = localPersistenceDir(shard.get());
                if(_args.updateDirHdfs==null) {
                    fact.createPersistence(
                            localLPDir,
                            options)
                         .close();
                } else {
                    String copyDir = _args.updateDirHdfs + "/" + shard;
                    if(!_fs.exists(new Path(copyDir))) {
                        fact.createPersistence(localLPDir,
                                               options)
                            .close();
                    } else {
                        ((ChecksumFileSystem)_fs).copyToLocalFile(new Path(copyDir), new Path(localLPDir), false);
                    }
                }
                lp = fact.openPersistenceForAppend(localLPDir, options);
                _lps.put(shard.get(), lp);
                progress();
            }

            _args.updater.updateElephant(lp, record.key, record.val);

            _numWritten++;
            if(_numWritten % 25000 == 0) {
                long now = System.currentTimeMillis();
                long delta = now - _lastCheckpoint;
                _lastCheckpoint = delta;
                LOG.info("Wrote last 25000 records in " + delta + " ms");
                _dirFlag.setLastModified(now);
            }
        }

        public void close(Reporter reporter) throws IOException {
            for(Integer shard: _lps.keySet()) {
                String lpDir = localPersistenceDir(shard);
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
            FileSystem.getLocal(new Configuration()).delete(new Path(_localRoot), true);
            _dirFlag.delete();
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
        if(fs.exists(new Path(conf.get(args.outputDirHdfs)))) {
            throw new InvalidJobConfException("Output dir already exists " + args.outputDirHdfs);
        }
        if(args.updateDirHdfs!=null && !fs.exists(new Path(conf.get(args.updateDirHdfs)))) {
            throw new InvalidJobConfException("Shards to update does not exist " + args.updateDirHdfs);
        }
    }
}
