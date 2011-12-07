package elephantdb.hadoop;

import elephantdb.DomainSpec;
import elephantdb.persistence.Document;
import elephantdb.persistence.LocalPersistence;

import java.io.IOException;
import java.io.Serializable;

//
//

/**
 *
 * interface Updater<P implements LocalPersistence, R implements Record>  {
 *     void index(P persistence, R record);
 * }
 * Record here needs a kryo serializer registered.
 */
public interface ElephantUpdater<D> extends Serializable {
    public void update(LocalPersistence lp, D doc) throws IOException;
}