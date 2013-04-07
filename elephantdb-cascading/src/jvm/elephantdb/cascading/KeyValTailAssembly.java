package elephantdb.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.partition.ShardingScheme;
import org.apache.hadoop.io.BytesWritable;
import org.apache.log4j.Logger;

import java.util.UUID;

public class KeyValTailAssembly extends SubAssembly {
    public static Logger LOG = Logger.getLogger(KeyValTailAssembly.class);

    public static class Shardize extends BaseOperation implements Function {
        ShardingScheme shardScheme;
        int shardCount;

        public Shardize(String outfield, DomainSpec spec) {
            super(new Fields(outfield));
            shardScheme = spec.getShardScheme();
            shardCount = spec.getNumShards();
        }

        public int shardIndex(byte[] key) {
            return shardScheme.shardIndex(key, shardCount);
        }

        public void operate(FlowProcess process, FunctionCall call) {
            Object key = call.getArguments().getObject(0);

            int shard = shardIndex((byte[])key);
            call.getOutputCollector().add(new Tuple(shard));
        }
    }

    public static class MakeSortableKey extends BaseOperation implements Function {

        public MakeSortableKey(String outfield, DomainSpec spec) {
            super(new Fields(outfield));
        }

        public void operate(FlowProcess process, FunctionCall call) {
            Object f1 = call.getArguments().getObject(0);
            byte[] key = (byte[]) f1;
            BytesWritable sortField = new BytesWritable(key);
            call.getOutputCollector().add(new Tuple(sortField));
        }
    }

    public KeyValTailAssembly(Pipe keyValuePairs, ElephantDBTap outTap) {
        // generate two random field names
        String shardField = "shard" + UUID.randomUUID().toString();
        String keySortField = "keysort" + UUID.randomUUID().toString();

        DomainSpec spec = outTap.getSpec();
        LOG.info("Instantiating spec: " + spec);

        // Add the shard index as field #2.
        Pipe out = new Each(keyValuePairs, new Fields(0), new Shardize(shardField, spec), Fields.ALL);

        // Add the serialized key itself as field #3 for sorting.
        // TODO: Make secondary sorting optional, and come up with a function to generate
        // a sortable key (vs just using the same serialization as for sharding).
        out = new Each(out, new Fields(0), new MakeSortableKey(keySortField, spec), Fields.ALL);

        //put in order of shard, key, value, sortablekey
        out = new Each(out, new Fields(2, 0, 1, 3), new Identity(), Fields.RESULTS);
        out = new GroupBy(out, new Fields(0), new Fields(3)); // group by shard

        // emit shard, key, value
        out = new Each(out, new Fields(0, 1, 2), new Identity());
        setTails(out);
    }
}
