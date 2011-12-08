package elephantdb.test;

import elephantdb.hadoop.ElephantUpdater;
import elephantdb.persistence.JavaBerkDB;
import elephantdb.persistence.KeyValDocument;

import java.io.IOException;

public class StringAppendUpdater implements ElephantUpdater<JavaBerkDB.JavaBerkDBPersistence, KeyValDocument> {

    public void update(JavaBerkDB.JavaBerkDBPersistence lp, KeyValDocument doc) throws IOException {
        Object oldVal = lp.get(doc.key);
        String newVal = (String) doc.value;

        String oldString = "";
        if (oldVal != null) {
            oldString = (String) oldVal;
        }

        doc.value = oldString + newVal;
        lp.index(doc);
    }
}
