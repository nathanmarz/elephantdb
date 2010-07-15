package elephantdb.persistence;

import java.io.IOException;

public interface LocalPersistence {
    public byte[] get(byte[] key) throws IOException;
    public void add(byte[] key, byte[] value) throws IOException;
    public void close() throws IOException;
}
