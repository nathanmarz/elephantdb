package elephantdb.persistence;

/** User: sritchie Date: 11/22/11 Time: 5:27 PM */
public interface Sharder {
    public int shardIndex(int numShards, Object key, Object val);
}
