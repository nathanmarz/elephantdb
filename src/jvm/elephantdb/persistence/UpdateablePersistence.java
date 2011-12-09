package elephantdb.persistence;

import elephantdb.hadoop.ElephantUpdater;

import java.io.IOException;

/**
 * User: sritchie
 * Date: 12/9/11
 * Time: 11:59 AM
 */
public abstract class UpdateablePersistence<D extends Document> implements LocalPersistence<D> {
    public void index(D document, ElephantUpdater<UpdateablePersistence<D>, D> updater) throws IOException {
        if (updater == null)
            index(document);
        else
            updater.update(this, document);
    }
}
