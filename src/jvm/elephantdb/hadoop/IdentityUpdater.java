package elephantdb.hadoop;

import elephantdb.persistence.Document;
import elephantdb.persistence.LocalPersistence;

import java.io.IOException;


/**
 * Does what you'd expect and just passes the k-v pairs right on through.
 */
public class IdentityUpdater<P extends LocalPersistence, D extends Document> implements ElephantUpdater<P, D> {

    public void update(P lp, D doc) throws IOException {
        lp.index(doc);
    }
}