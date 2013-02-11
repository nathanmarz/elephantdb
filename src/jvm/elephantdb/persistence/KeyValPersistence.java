package elephantdb.persistence;

import elephantdb.document.KeyValDocument;

import java.io.IOException;

public interface KeyValPersistence extends Persistence<KeyValDocument> {
    byte[] get(byte[] key) throws IOException;
    void put(byte[] key, byte[] value) throws IOException;
}
