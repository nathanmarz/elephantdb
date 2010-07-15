package elephantdb.client;

import elephantdb.generated.DomainNotFoundException;
import elephantdb.generated.DomainNotLoadedException;
import elephantdb.generated.HostsDownException;
import elephantdb.generated.Value;

public interface IElephantClient {
    Value get(String domain, byte[] key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    Value getString(String domain, String key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    Value getInt(String domain, int key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
    Value getLong(String domain, long key)
            throws DomainNotLoadedException, DomainNotFoundException, HostsDownException;
}