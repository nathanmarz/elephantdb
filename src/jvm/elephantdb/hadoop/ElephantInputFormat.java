package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.CloseableIterator;
import elephantdb.persistence.LocalPersistence;
import elephantdb.persistence.LocalPersistence.KeyValuePair;
import elephantdb.store.DomainStore;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class ElephantInputFormat implements InputFormat<BytesWritable, BytesWritable> {
    public static final String ARGS_CONF = "elephant.output.args";

    public static class Args implements Serializable {
        public Map<String, Object> persistenceOptions = new HashMap<String, Object>();
        public String inputDirHdfs;
        public List<String> tmpDirs = new ArrayList<String>() {{
            add("/tmp");
        }};
        public Long version = null;

        public Args(String inputDirHdfs) {
            this.inputDirHdfs = inputDirHdfs;
        }
        
        public void setTmpDirs(List<String> dirs) {
            this.tmpDirs = dirs;
        }
    }

    public static class ElephantRecordReader implements RecordReader<BytesWritable, BytesWritable> {
        ElephantInputSplit _split;
        Reporter _reporter;
        Args _args;
        LocalElephantManager _manager;
        LocalPersistence _lp;
        CloseableIterator<KeyValuePair> _iterator;
        boolean finished = false;
        int numRead = 0;

        public ElephantRecordReader(ElephantInputSplit split, Reporter reporter) throws IOException {
            _split = split;
            _reporter = reporter;
            _args = (Args) Utils.getObject(_split.conf, ARGS_CONF);
            _manager = new LocalElephantManager(Utils.getFS(_split.shardPath, split.conf),
                    _split.spec, _args.persistenceOptions, _args.tmpDirs);
            String localpath = _manager.downloadRemoteShard("shard", _split.shardPath);
           _lp = _split.spec.getLPFactory().openPersistenceForRead(localpath, _args.persistenceOptions);
           _iterator = _lp.iterator();
        }

        public boolean next(BytesWritable k, BytesWritable v) throws IOException {
            if(_iterator.hasNext()) {
                KeyValuePair pair = _iterator.next();
                k.set(pair.key, 0, pair.key.length);
                v.set(pair.value, 0, pair.value.length);
                numRead++;
                if(_reporter!=null) _reporter.progress();
                return true;
            } else {
                if(_reporter!=null) _reporter.progress();
                return false;
            }
        }

        public BytesWritable createKey() {
            return new BytesWritable();
        }

        public BytesWritable createValue() {
            return new BytesWritable();
        }

        public long getPos() throws IOException {
            return numRead;
        }

        public void close() throws IOException {
            _iterator.close();
            _lp.close();
            _manager.cleanup();
        }

        public float getProgress() throws IOException {
            if(finished) {
                return 1;
            } else {
                return 0;
            }
        }        
    }

    public static class ElephantInputSplit implements InputSplit {
        private String shardPath;
        private DomainSpec spec;
        private JobConf conf;

        public ElephantInputSplit() {
            
        }

        public ElephantInputSplit(String shardPath, DomainSpec spec, JobConf conf) {
            this.shardPath = shardPath;
            this.spec = spec;
            this.conf = conf;
        }

        public long getLength() throws IOException {
            FileSystem fs = Utils.getFS(shardPath, conf);
            return fs.getContentSummary(new Path(shardPath)).getLength();
        }

        public String[] getLocations() throws IOException {
            // TODO: look at a file in the shardpath
            return new String[] {};
        }

        public void write(DataOutput d) throws IOException {
            spec.write(d);
            WritableUtils.writeString(d, shardPath);
            conf.write(d);
        }

        public void readFields(DataInput di) throws IOException {
            spec = new DomainSpec();
            spec.readFields(di);
            shardPath = WritableUtils.readString(di);
            conf = new JobConf();
            conf.readFields(di);
        }     
    }


    public InputSplit[] getSplits(JobConf jc, int ignored) throws IOException {
        Args args = (Args) Utils.getObject(jc, ARGS_CONF);
        FileSystem fs = Utils.getFS(args.inputDirHdfs, jc);
        DomainStore store = new DomainStore(fs, args.inputDirHdfs);
        String versionPath;
        if(args.version==null) {
            versionPath = store.mostRecentVersionPath();
        } else {
            versionPath = store.versionPath(args.version);
        }
        DomainSpec spec = store.getSpec();
        List<InputSplit> ret = new ArrayList<InputSplit>();
        for(int i=0; i<spec.getNumShards(); i++) {
            String shardPath = versionPath + "/" + i;
            if(fs.exists(new Path(shardPath))) {
                ret.add(new ElephantInputSplit(shardPath, spec, jc));
            }
        }
        return ret.toArray(new InputSplit[ret.size()]);
    }


    public RecordReader<BytesWritable, BytesWritable> getRecordReader(InputSplit is, JobConf jc, Reporter reporter) throws IOException {
        return new ElephantRecordReader((ElephantInputSplit) is, reporter);
    }
}
