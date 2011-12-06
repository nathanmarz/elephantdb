package elephantdb.persistence;

import elephantdb.DomainSpec;

/** User: sritchie Date: 12/6/11 Time: 2:06 PM */
public abstract class ShardScheme {
    DomainSpec _spec;

    public void assertSpec() {
        if (_spec == null)
            throw new RuntimeException("spec hasn't been instantiated!");
    }
    
    public void setSpec(DomainSpec spec) {
        _spec = spec;
    }

    public DomainSpec getSpec() {
        return _spec;
    }

    public abstract int shardIndex(Object shardKey);
}
