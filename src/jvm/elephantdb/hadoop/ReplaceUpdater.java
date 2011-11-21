package elephantdb.hadoop;

import elephantdb.persistence.LocalPersistence;

import java.io.IOException;


/**
 * Does what you'd expect and just passes the k-v pairs right on through.
 */
public class ReplaceUpdater implements ElephantUpdater {
    public void updateElephant(LocalPersistence lp, byte[] newKey, byte[] newVal)
        throws IOException {
        lp.add(newKey, newVal);
    }

}
