package elephantdb.hadoop;

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
public interface ElephantUpdater extends Serializable {
    public void updateElephant(LocalPersistence lp, byte[] newKey, byte[] newVal)
        throws IOException;
}