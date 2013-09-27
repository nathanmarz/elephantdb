package elephantdb.client;

import elephantdb.generated.DomainStatus;
import elephantdb.generated.Status;
import elephantdb.generated.Value;
import elephantdb.generated.keyval.ElephantDB;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class encapsulates some of the Thrift ugliness. Create it with the host and port of your ElephantDB
 * cluster. Be sure to call close() when you're done or use it inside a try-with-resources.
 */
public class ElephantDbClient implements AutoCloseable {
    private Logger logger = LoggerFactory.getLogger(ElephantDbClient.class);

    private TFramedTransport transport;
    private ElephantDB.Client client;

    public ElephantDbClient(String host, int port) {
        transport = new TFramedTransport(new TSocket(host, port));
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        client = new ElephantDB.Client(protocol);
    }

    /**
     * Get the value of the given key.
     */
    public byte[] get(String domain, byte[] key) {
        Value value;

        try {
            value = client.get(domain, ByteBuffer.wrap(key));
        } catch (Exception e) {
            logger.error("Caught {} exception while getting value from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        if (value != null) {
            return value.get_data();
        } else {
            return null;
        }
    }

    /**
     * Gets the provided keys from ElephantDB on the given host and port from the given domain.
     */
    public Map<byte[], byte[]> multiGet(String domain, Set<ByteBuffer> keySet) {
        Map<ByteBuffer, Value> results;

        try {
            results = client.multiGet(domain, keySet);
        } catch (Exception e) {
            logger.error("Caught {} exception while multi-getting values from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        Map<byte[], byte[]> returnVal = new HashMap<>();

        if (results != null) {
            returnVal = parseResultsMap(results);
        }

        return returnVal;
    }

    /* Parse the results map returned from the client into a map of byte arrays */
    private Map<byte[], byte[]> parseResultsMap(Map<ByteBuffer, Value> results) {
        Map<byte[], byte[]> returnVal = new HashMap<>();

        for (Map.Entry entry : results.entrySet()) {
            Value value = (Value) entry.getValue();
            ByteBuffer keyBuffer = (ByteBuffer) entry.getKey();
            int remaining = keyBuffer.remaining();
            byte[] newByteArray = new byte[remaining];
            keyBuffer.get(newByteArray);
            returnVal.put(newByteArray, value.get_data());
        }

        return returnVal;
    }

    /**
     * Get the list of domains available
     */
    public List<String> getDomains() {
        List<String> domains;

        try {
            domains = client.getDomains();
        } catch (Exception e) {
            logger.error("Caught {} exception while getting domain list from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        return domains;
    }

    /**
     * Get the status of all domains.
     */
    public Map<String, DomainStatus> getStatus() {
        Map<String, DomainStatus> domainStatuses;

        try {
            Status statuses = client.getStatus();
            domainStatuses = statuses.get_domain_statuses();
        } catch (Exception e) {
            logger.error("Caught {} exception while getting all domain statuses from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        return domainStatuses;
    }

    /**
     * Get the status of a domain
     */
    public DomainStatus getDomainStatus(String domain) {
        DomainStatus status;

        try {
            status = client.getDomainStatus(domain);
        } catch (Exception e) {
            logger.error("Caught {} exception while getting domain status for domain {} from ElephantDB.", domain);
            throw new RuntimeException(e);
        }

        return status;
    }

    /**
     * Check if all domains are fully loaded.
     */
    public boolean fullyLoaded() {
        boolean result;

        try {
            result = client.isFullyLoaded();
        } catch (Exception e) {
            logger.error("Caught {} exception while getting fully loaded status from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Check if any domains are currently updating.
     */
    public boolean updating() {
        boolean result;

        try {
            result = client.isUpdating();
        } catch (Exception e) {
            logger.error("Caught {} exception while getting updating status from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Updates the named domain if an update is available
     */
    public boolean update(String domain) {
        boolean result;

        try {
            result = client.update(domain);
        } catch (Exception e) {
            logger.error("Caught {} exception while updating domain {} status from ElephantDB.", e.getMessage(), domain);
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * If an update is available on any domain, updates the domain's and hotswaps in the new versions.
     */
    public boolean updateAll() {
        boolean result;

        try {
            result = client.updateAll();
        } catch (Exception e) {
            logger.error("Caught {} exception while updating all domains from ElephantDB.", e.getMessage());
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Close the connection to ElephantDB
     */
    public void close() {
        transport.close();
    }
}
