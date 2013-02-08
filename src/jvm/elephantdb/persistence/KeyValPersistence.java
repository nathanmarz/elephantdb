package elephantdb.persistence;

import elephantdb.document.KeyValDocument;

import java.io.IOException;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 3:34 PM
 */
public interface KeyValPersistence extends Persistence<KeyValDocument> {
    <K, V> V get(K key) throws IOException;
    <K, V> void put(K key, V value) throws IOException;
}
