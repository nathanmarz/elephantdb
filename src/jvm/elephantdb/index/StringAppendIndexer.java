package elephantdb.index;

import elephantdb.index.Indexer;
import elephantdb.persistence.JavaBerkDB;
import elephantdb.persistence.KeyValDocument;

import java.io.IOException;

public class StringAppendIndexer implements Indexer<JavaBerkDB.JavaBerkDBPersistence, KeyValDocument> {

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