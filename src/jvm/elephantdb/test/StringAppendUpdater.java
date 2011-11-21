package elephantdb.test;

import elephantdb.hadoop.ElephantUpdater;
import elephantdb.persistence.LocalPersistence;

import java.io.IOException;

public class StringAppendUpdater implements ElephantUpdater {
    public void updateElephant(LocalPersistence lp, byte[] newKey, byte[] newVal)
        throws IOException {
        byte[] oldval = lp.get(newKey);
        String olds = "";
        if (oldval != null) { olds = new String(oldval); }
        lp.add(newKey, (olds + new String(newVal)).getBytes());
    }
}
