package elephantdb.hadoop;

import elephantdb.persistence.LocalPersistence;
import java.io.IOException;
import java.io.Serializable;


public interface ElephantUpdater extends Serializable {
    public void updateElephant(LocalPersistence lp, byte[] newKey, byte[] newVal) throws IOException;
}
