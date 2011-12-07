package elephantdb.persistence;

import elephantdb.DomainSpec;

/** User: sritchie Date: 12/7/11 Time: 9:38 AM */
public abstract class SpecifiedObject {
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
}
