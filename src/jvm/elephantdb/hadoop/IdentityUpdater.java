package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.persistence.KeyValDocument;
import elephantdb.persistence.LocalPersistence;

import java.io.IOException;


/**
 * Does what you'd expect and just passes the k-v pairs right on through.
 */
public class IdentityUpdater implements ElephantUpdater<KeyValDocument> {

    public void update(LocalPersistence lp, KeyValDocument doc) throws IOException {
        lp.index(doc);
    }
}