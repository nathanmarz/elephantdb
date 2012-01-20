package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.CloseableIterator;
import elephantdb.persistence.Persistence;
import elephantdb.serialize.Serializer;
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
import java.util.List;
import java.util.Map;

public class ElephantInputFormat implements InputFormat<NullWritable, BytesWritable> {
    public static final String ARGS_CONF = "elephant.input.args";

    public static class Args implements Serializable {
        public String inputDirHdfs;
        public Long version = null;

        public Args(String inputDirHdfs) {
            this.inputDirHdfs = inputDirHdfs;
        }
    }

    public static class ElephantRecordReader implements RecordReader<NullWritable, BytesWritable> {
        ElephantInputSplit split;
        Reporter reporter;
        Args args;
        LocalElephantManager elephantManager;
        Persistence lp;
        CloseableIterator iterator;
        boolean finished = false;
        int numRead = 0;

        public ElephantRecordReader(ElephantInputSplit split, Reporter reporter)
                throws IOException {
            this.split = split;
            this.reporter = reporter;
            args = (Args) Utils.getObject(this.split.conf, ARGS_CONF);
            elephantManager = new LocalElephantManager(
                    Utils.getFS(this.split.shardPath, split.conf), this.split.spec,
                    LocalElephantManager.getTmpDirs(this.split.conf));
            String localpath = elephantManager.downloadRemoteShard("shard", this.split.shardPath);

            Map<String, Object> opts = this.split.spec.getPersistenceOptions();
            lp = this.split.spec.getCoordinator().openPersistenceForRead(localpath, opts);

            iterator = lp.iterator();
        }

        // Okay, this now adds the actual Document to the BytesWritable value and the
        // shard index as an IntWritable key.
        public boolean next(NullWritable k, BytesWritable v) throws IOException {
            if (iterator.hasNext()) {
                Object pair = iterator.next();

                byte[] crushed = split.getSerializer().serialize(pair);
                v.set(new BytesWritable(crushed));

                numRead++;
                if (reporter != null) {
                    reporter.progress();
                }

                return true;
            } else {
                if (reporter != null) { reporter.progress(); }
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
            iterator.close();
            lp.close();
            elephantManager.cleanup();
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
        private Serializer serializer;
        private JobConf conf;

        public ElephantInputSplit() {
        }

        public ElephantInputSplit(String shardPath, DomainSpec spec, JobConf conf) {
            this.shardPath = shardPath;
            this.spec = spec;
            this.conf = conf;
        }
        
        public Serializer getSerializer() {
            if (serializer == null)
                serializer = Utils.makeSerializer(spec);

            return serializer;
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