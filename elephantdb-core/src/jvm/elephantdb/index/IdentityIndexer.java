package elephantdb.index;

import elephantdb.persistence.Persistence;

import java.io.IOException;


/**
 * Does what you'd expect and just passes the k-v pairs right on through.
 */
public class IdentityIndexer<P extends Persistence, D> implements Indexer<P, D> {

    public void index(P persistence, D doc) throws IOException {
        persistence.index(doc);
    }
}