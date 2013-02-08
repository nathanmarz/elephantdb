package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.index.IdentityIndexer;
import elephantdb.index.Indexer;
import elephantdb.persistence.Coordinator;
import elephantdb.persistence.Persistence;
import elephantdb.serialize.Serializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.Closeable;
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

        // Implements the Indexer interface.
        public Indexer indexer = new IdentityIndexer();

        // If this is set, the output format will go download it!
        public String updateDirHdfs = null;

        public Args(DomainSpec spec, String outputDirHdfs) {
            this.spec = spec;
            this.outputDirHdfs = outputDirHdfs;
        }
    }

    public class ElephantRecordWriter implements RecordWriter<IntWritable, BytesWritable>, Closeable {

        FileSystem fileSystem;
        Args args;
        Map<Integer, Persistence> lps = new HashMap<Integer, Persistence>();
        Progressable progressable;
        LocalElephantManager localManager;
        Serializer serializer;

        int numWritten = 0;
        long lastCheckpoint = System.currentTimeMillis();

        public ElephantRecordWriter(Configuration conf, Args args, Progressable progressable)
            throws IOException {
            fileSystem = Utils.getFS(args.outputDirHdfs, conf);
            this.args = args;

            this.serializer = Utils.makeSerializer(this.args.spec);
            this.progressable = progressable;
            localManager = new LocalElephantManager(fileSystem, args.spec, LocalElephantManager.getTmpDirs(conf));
        }

        private String remoteUpdateDirForShard(int shard) {
            return (args.updateDirHdfs == null)? null : args.updateDirHdfs + "/" + shard;
        }
        
        private Persistence retrieveShard(int shardIdx) throws IOException {
            Persistence lp = null;

            if (lps.containsKey(shardIdx)) {
                lp = lps.get(shardIdx);
            } else {
                String updateDir = remoteUpdateDirForShard(shardIdx);
                String localShard = localManager.downloadRemoteShard("" + shardIdx, updateDir);

                Coordinator fact = args.spec.getCoordinator();
                lp = fact.openPersistenceForAppend(localShard, args.spec.getPersistenceOptions());

                lps.put(shardIdx, lp);
                progress();
            }
            return lp;
        }

        public void write(IntWritable shard, BytesWritable carrier) throws IOException {
            Persistence lp = retrieveShard(shard.get());

            // TODO: Change this behavior and get Cascading to serialize object.
            Object doc = serializer.deserialize(Utils.getBytes(carrier));

            if (args.indexer != null) {
                args.indexer.index(lp, doc);
            } else {
                lp.index(doc);
            }

            bumpProgress();
        }

        public void bumpProgress() {
            numWritten++;
            if (numWritten % 25000 == 0) {
                long now = System.currentTimeMillis();
                long delta = now - lastCheckpoint;
                lastCheckpoint = now;
                LOG.info("Wrote last 25000 records in " + delta + " ms");
                localManager.progress();
            }
        }
        
        public void close() throws IOException {
            close(null);
        }

        public void close(Reporter reporter) throws IOException {
            for (Integer shard : lps.keySet()) {
                String lpDir = localManager.localTmpDir("" + shard);
                LOG.info("Closing LP for shard " + shard + " at " + lpDir);
                lps.get(shard).close();
                LOG.info("Closed LP for shard " + shard + " at " + lpDir);
                progress();
                String remoteDir = args.outputDirHdfs + "/" + shard;
                
                // Do all this stuff to ensure that S3 actually does delete
                int deleteAttempt = 4;
                while(fileSystem.exists(new Path(remoteDir)) && deleteAttempt > 0) {
                    LOG.info("Deleting existing shard " + shard + " at " + remoteDir);
                    fileSystem.delete(new Path(remoteDir), true);
                    --deleteAttempt;
                }
                if (fileSystem.exists(new Path(remoteDir)) && deleteAttempt == 0) {
                    throw new IOException("Failed to delete shard " + shard + " at " + remoteDir
                            + " after " + deleteAttempt + " attempts!");
                } else {
                    LOG.info("Deleted existing shard " + shard + " at " + remoteDir);
                }
                LOG.info("Copying " + lpDir + " to " + remoteDir);
                fileSystem.copyFromLocalFile(new Path(lpDir), new Path(remoteDir));
                LOG.info("Copied " + lpDir + " to " + remoteDir);
                progress();
            }
            localManager.cleanup();
        }

        private void progress() {
            if (progressable != null)
                progressable.progress();
        }
    }

    public RecordWriter<IntWritable, BytesWritable> getRecordWriter
            (FileSystem fs,JobConf conf, String string, Progressable progressable)
            throws IOException {
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
