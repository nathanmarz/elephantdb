package elephantdb.partition;

import elephantdb.Utils;
import elephantdb.serialize.KryoSerializer;
import elephantdb.serialize.SerializationWrapper;
import elephantdb.serialize.Serializer;

/** User: sritchie Date: 11/22/11 Time: 5:32 PM */
public class HashModScheme implements ShardingScheme, SerializationWrapper {
    Serializer serializer;

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public int shardIndex(Object shardKey, int shardCount) {
        byte[] serializedKey = getSerializer().serialize(shardKey);

        return Utils.keyShard(serializedKey, shardCount);
    }
}
