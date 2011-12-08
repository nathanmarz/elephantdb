package elephantdb.hadoop;

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
public interface ElephantUpdater<P, D> extends Serializable {
    public void update(P localPersistence, D doc) throws IOException;
}