package elephantdb.index;

import elephantdb.document.KeyValDocument;
import elephantdb.persistence.JavaBerkDB;

import java.io.IOException;

public class StringAppendIndexer implements Indexer<JavaBerkDB.JavaBerkDBPersistence, KeyValDocument> {

    public void index(JavaBerkDB.JavaBerkDBPersistence lp, KeyValDocument doc) throws IOException {
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