package elephantdb.partition;

import elephantdb.Utils;
import elephantdb.serialize.SerializationWrapper;
import elephantdb.serialize.Serializer;
import org.apache.log4j.Logger;

/** User: sritchie Date: 11/22/11 Time: 5:32 PM */
public class HashModScheme implements ShardingScheme, SerializationWrapper {
    public static final Logger LOG = Logger.getLogger(HashModScheme.class);

    Serializer serializer;

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public int shardIndex(Object shardKey, int shardCount) {
        LOG.debug("shardIndex for object " + shardKey);
        byte[] serializedKey = getSerializer().serialize(shardKey);
        
        LOG.debug("shardIndex returned " + serializedKey.length + " bytes");
        return Utils.keyShard(serializedKey, shardCount);
    }
}
