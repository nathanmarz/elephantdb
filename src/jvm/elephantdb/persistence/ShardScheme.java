package elephantdb.persistence;

import elephantdb.DomainSpec;

/** User: sritchie Date: 12/6/11 Time: 2:06 PM */
public abstract class ShardScheme extends SpecifiedObject {
    public abstract int shardIndex(Object shardKey);
}
