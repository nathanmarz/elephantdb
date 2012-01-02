package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.document.Document;
import elephantdb.persistence.*;
import elephantdb.store.DomainStore;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElephantInputFormat implements InputFormat<NullWritable, BytesWritable> {
    public static final String ARGS_CONF = "elephant.output.args";

    public static class Args implements Serializable {
        public Map<String, Object> persistenceOptions = new HashMap<String, Object>();
        public String inputDirHdfs;
        public Long version = null;

        public Args(String inputDirHdfs) {
            this.inputDirHdfs = inputDirHdfs;
        }
    }

    public static class ElephantRecordReader implements RecordReader<NullWritable, BytesWritable> {
        ElephantInputSplit _split;
        Reporter _reporter;
        Args _args;
        LocalElephantManager _manager;
        Persistence _lp;
        CloseableIterator _iterator;
        boolean finished = false;
        int numRead = 0;

        public ElephantRecordReader(ElephantInputSplit split, Reporter reporter)
                throws IOException {
            _split = split;
            _reporter = reporter;
            _args = (Args) Utils.getObject(_split.conf, ARGS_CONF);
            _manager = new LocalElephantManager(
                    Utils.getFS(_split.shardPath, split.conf), _split.spec, _args.persistenceOptions,
                    LocalElephantManager.getTmpDirs(_split.conf));
            String localpath = _manager.downloadRemoteShard("shard", _split.shardPath);
            _lp = _split.spec.getCoordinator().openPersistenceForRead(localpath, _args.persistenceOptions);
            _iterator = _lp.iterator();
        }

        // Okay, this now adds the actual Document to the BytesWritable value and the
        // shard index as an IntWritable key.
        public boolean next(NullWritable k, BytesWritable v) throws IOException {
            if (_iterator.hasNext()) {
                Document pair = (Document) _iterator.next();

                //TODO: At this point we need to run this through some sort of "fetcher" that can
                //build the key-val document back up from what pops out of berkeleyDB.

                byte[] crushed = _split.getKryoBuffer().serialize(pair);
                v.set(new BytesWritable(crushed));

                numRead++;
                if (_reporter != null) {
                    _reporter.progress();
                }

                return true;
            } else {
                if (_reporter != null) { _reporter.progress(); }
                return false;
            }
        }

        public NullWritable createKey() {
            return NullWritable.get();
        }

        public BytesWritable createValue() {
            return new BytesWritable();
        }

        public long getPos() throws IOException {
            // TODO: switch this to use a property on the iterator.
            return numRead;
        }

        public void close() throws IOException {
            _iterator.close();
            _lp.close();
            _manager.cleanup();
        }

        public float getProgress() throws IOException {
            if (finished) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * PROBLEMS HERE: We have to have some way of generating more than one split for every
     * shard.
     */
    public static class ElephantInputSplit implements InputSplit {
        private String shardPath;
        private DomainSpec spec;
        private KryoBuffer kryoBuf;
        private JobConf conf;

        public ElephantInputSplit() {
        }

        public ElephantInputSplit(String shardPath, DomainSpec spec, JobConf conf) {
            this.shardPath = shardPath;
            this.spec = spec;
            this.conf = conf;
        }
        
        public KryoBuffer getKryoBuffer() {
            // TODO: Remove the cast and make this work with interfaces.
            if (kryoBuf == null)
                kryoBuf = ((KryoWrapper) spec.getCoordinator()).getKryoBuffer();

            return kryoBuf;
        }

        // TODO: Store this result in a variable and use it to update the
        // percentage complete.
        public long getLength() throws IOException {
            FileSystem fs = Utils.getFS(shardPath, conf);
            return fs.getContentSummary(new Path(shardPath)).getLength();
        }

        public String[] getLocations() throws IOException {
            // TODO: look at a file in the shardpath
            return new String[]{};
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
        if (args.version == null) {
            versionPath = store.mostRecentVersionPath();
        } else {
            versionPath = store.versionPath(args.version);
        }
        DomainSpec spec = store.getSpec();
        List<InputSplit> ret = new ArrayList<InputSplit>();
        for (int i = 0; i < spec.getNumShards(); i++) {
            String shardPath = versionPath + "/" + i;
            if (fs.exists(new Path(shardPath))) {
                ret.add(new ElephantInputSplit(shardPath, spec, jc));
            }
        }
        return ret.toArray(new InputSplit[ret.size()]);
    }


    public RecordReader<NullWritable, BytesWritable> getRecordReader(InputSplit is, JobConf jc,
                                                                     Reporter reporter) throws IOException {
        return new ElephantRecordReader((ElephantInputSplit) is, reporter);
    }
}