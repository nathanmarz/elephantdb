package elephantdb.persistence;

/** User: sritchie Date: 11/22/11 Time: 5:27 PM
 *
 * A Sharder takes in key and value objects and returns a shard index. Search, for example;
 * A LuceneSharder might be configured to shard against the "user" field in a document (value).
 *
 * For lucene, the key doesn't really matter at all. In wonderdog it's null, actually.
 *
 * */
public interface Sharder {
    public int shardIndex(int numShards, Object key, Object val);
}
