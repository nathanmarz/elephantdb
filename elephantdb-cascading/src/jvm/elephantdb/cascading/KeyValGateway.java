package elephantdb.cascading;

import cascading.tuple.Tuple;
import elephantdb.document.KeyValDocument;

/** User: sritchie Date: 12/16/11 Time: 12:08 AM */
public class KeyValGateway implements Gateway<KeyValDocument> {
    public KeyValDocument fromTuple(Tuple tuple) {
        Object f1 = tuple.getObject(1);
        Object f2 = tuple.getObject(2);
        
        byte[] key = (byte[]) f1;
        byte[] val = (byte[]) f2;

        return new KeyValDocument(key, val);
    }

    public Tuple toTuple(KeyValDocument document) {
        return new Tuple(document.key, document.value);
    }
}
