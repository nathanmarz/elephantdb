package elephantdb.hadoop;

import elephantdb.persistence.LocalPersistence;
import java.io.IOException;


public class ReplaceUpdater implements ElephantUpdater {
    public void updateElephant(LocalPersistence lp, byte[] newKey, byte[] newVal) throws IOException {
        lp.add(newKey, newVal);
    }

}
