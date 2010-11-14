package elephantdb.client;

import elephantdb.generated.DomainNotFoundException;
import elephantdb.generated.DomainNotLoadedException;
import elephantdb.generated.HostsDownException;
import elephantdb.generated.Value;
import java.util.List;

public interface IElephantClient {
    Value get(String domain, byte[] key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    Value getString(String domain, String key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    Value getInt(String domain, int key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    Value getLong(String domain, long key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;

    List<Value> multiGet(String domain, List<byte[]> key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    List<Value> multiGetString(String domain, List<String> key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    List<Value> multiGetInt(String domain, List<Integer> key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    List<Value> multiGetLong(String domain, List<Long> key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
}