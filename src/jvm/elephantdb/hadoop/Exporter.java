package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.persistence.KeySorter;
import elephantdb.store.DomainStore;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Not really sure what this class is here for.
 */
public class Exporter {
    public static class CompoundKey implements Writable {
        int shard;
        byte[] shardKey;


        public CompoundKey() {

        }

        public CompoundKey(int shard, byte[] shardKey) {
            this.shard = shard;
            this.shardKey = shardKey;
        }

        public void write(DataOutput d) throws IOException {
            WritableUtils.writeVInt(d, shard);
            WritableUtils.writeVInt(d, shardKey.length);
            d.write(shardKey);
        }

        public void readFields(DataInput di) throws IOException {
            shard = WritableUtils.readVInt(di);
            shardKey = new byte[WritableUtils.readVInt(di)];
            di.readFully(shardKey);
        }

    }


    public static class ExporterMapper
        implements Mapper<BytesWritable, BytesWritable, CompoundKey, ElephantRecordWritable> {
        KeySorter sorter;
        int _numShards;

        public void map(BytesWritable key, BytesWritable val,
            OutputCollector<CompoundKey, ElephantRecordWritable> oc, Reporter rprtr)
            throws IOException {
            byte[] keyBytes = Utils.getBytes(key);
            byte[] sortKey = sorter.getSortableKey(keyBytes);
            byte[] valBytes = Utils.getBytes(val);
            int shard = Utils.keyShard(keyBytes, _numShards);
            oc.collect(new CompoundKey(shard, sortKey), new ElephantRecordWritable(keyBytes, valBytes));
        }

        public void configure(JobConf jc) {
            ElephantOutputFormat.Args args =
                (ElephantOutputFormat.Args) Utils.getObject(jc, ElephantOutputFormat.ARGS_CONF);
            _numShards = args.spec.getNumShards();
            sorter = args.spec.getCoordinator().getKeySorter();
        }

        public void close() throws IOException {
        }

    }

    public static class ExporterReducer implements
        Reducer<CompoundKey, ElephantRecordWritable, IntWritable, ElephantRecordWritable> {
        IntWritable shard = new IntWritable();


        public void reduce(CompoundKey key, Iterator<ElephantRecordWritable> it,
            OutputCollector<IntWritable, ElephantRecordWritable> oc, Reporter rprtr)
            throws IOException {
            while (it.hasNext()) {
                shard.set(key.shard);
                oc.collect(shard, it.next());
            }
        }

        public void configure(JobConf jc) {
        }

        public void close() throws IOException {
        }

    }

    public static class ElephantPartitioner
        implements Partitioner<CompoundKey, ElephantRecordWritable> {

        public int getPartition(CompoundKey k2, ElephantRecordWritable v2, int numPartitions) {
            return k2.shard % numPartitions;
        }

        public void configure(JobConf jc) {
        }
    }

    public static class Args {
        public DomainSpec spec;

        // set updater to null to disable any incremental updates. This will force new version to only contain the
        // K/V pairs you give it
        public ElephantUpdater updater = new ReplaceUpdater();
        public Map<String, Object> persistenceOptions = new HashMap<String, Object>();

        public Args(DomainSpec spec) {
            this.spec = spec;
        }
    }

    public static final class ElephantPrimarySort implements RawComparator<CompoundKey> {

        /**
         *
         */
        public int compare(byte[] key1, int start1, int length1, byte[] key2, int start2, int length2) {

            //
            DataInputStream s1 = new DataInputStream(new ByteArrayInputStream(key1, start1, length1));
            DataInputStream s2 = new DataInputStream(new ByteArrayInputStream(key2, start2, length2));
            try {
                return WritableUtils.readVInt(s1) - WritableUtils.readVInt(s2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        public int compare(CompoundKey key1, CompoundKey key2) {
            return key1.shard - key2.shard;
        }
    }


    public static final class ElephantSecondarySort implements RawComparator<CompoundKey> {
        CompoundKey ckey1 = new CompoundKey();
        CompoundKey ckey2 = new CompoundKey();

        public int compare(byte[] key1, int start1, int length1, byte[] key2, int start2, int length2) {
            try {
                DataInputStream s1 = new DataInputStream(new ByteArrayInputStream(key1, start1, length1));
                DataInputStream s2 = new DataInputStream(new ByteArrayInputStream(key2, start2, length2));
                ckey1.readFields(s1);
                ckey2.readFields(s2);
                return compare(ckey1, ckey2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int compare(CompoundKey key1, CompoundKey key2) {
            if (key1.shard != key2.shard) {
                return key1.shard - key2.shard;
            } else {
                // why not just do this above, then call the compare method here?
                return WritableComparator.compareBytes(key1.shardKey, 0, key1.shardKey.length, key2.shardKey, 0, key2.shardKey.length);
            }
        }
    }

    public static void export(String srcDir, String outDir, Args args)
        throws IOException, InterruptedException {
        DomainStore store = new DomainStore(outDir, args.spec);
        String newVersion = store.createVersion();
        String oldVersion = store.mostRecentVersionPath();
        ElephantOutputFormat.Args jobargs = new ElephantOutputFormat.Args(args.spec, newVersion);
        jobargs.persistenceOptions = args.persistenceOptions;
        if (args.updater != null) {
            jobargs.updater = args.updater;
            jobargs.updateDirHdfs = oldVersion;
        }

        JobConf conf = new JobConf();
        SequenceFileInputFormat.setInputPaths(conf, srcDir);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setMapOutputKeyClass(CompoundKey.class);
        conf.setMapOutputValueClass(ElephantRecordWritable.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(ElephantRecordWritable.class);
        conf.setOutputFormat(ElephantOutputFormat.class);
        conf.setMapperClass(ExporterMapper.class);
        conf.setReducerClass(ExporterReducer.class);
        conf.setPartitionerClass(ElephantPartitioner.class);

        conf.setOutputValueGroupingComparator(ElephantPrimarySort.class);
        conf.setOutputKeyComparatorClass(ElephantSecondarySort.class);

        conf.setSpeculativeExecution(false);
        conf.setNumReduceTasks(args.spec.getNumShards());

        conf.setJobName("EDB Exporter: " + srcDir + " -> " + newVersion);
        Utils.setObject(conf, ElephantOutputFormat.ARGS_CONF, jobargs);

        try {
            RunningJob job = new JobClient(conf).submitJob(conf);
            while (!job.isComplete()) {
                Thread.sleep(100);
            }

            if (!job.isSuccessful()) { throw new IOException("BalancedDistcp failed"); }
            //run job
            if (oldVersion != null) {
                DomainStore.synchronizeVersions(Utils.getFS(outDir, conf), args.spec, oldVersion, newVersion);
            }
            store.succeedVersion(newVersion);
        } catch (IOException e) {
            store.failVersion(newVersion);
            throw e;
        }

    }

    public static void export(String srcDir, String outDir, DomainSpec spec)
        throws IOException, InterruptedException {
        export(srcDir, outDir, new Args(spec));
    }

}
