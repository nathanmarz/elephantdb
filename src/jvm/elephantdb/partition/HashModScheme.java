package elephantdb.partition;

import elephantdb.Utils;
import elephantdb.persistence.KryoBuffer;
import elephantdb.persistence.KryoWrapper;

/** User: sritchie Date: 11/22/11 Time: 5:32 PM */
public class HashModScheme implements ShardingScheme, KryoWrapper {
    private KryoBuffer kryoBuf;

    public void setKryoBuffer(KryoBuffer buffer) {
        kryoBuf = buffer;
    }

    public KryoBuffer getKryoBuffer() {
        return kryoBuf;
    }

    public int shardIndex(Object shardKey, int shardCount) {
        byte[] serializedKey = getKryoBuffer().serialize(shardKey);

        return Utils.keyShard(serializedKey, shardCount);
    }
}
