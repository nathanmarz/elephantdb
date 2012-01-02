package elephantdb.persistence;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 12:37 PM
 */
public interface KryoWrapper {
    void setKryoBuffer(KryoBuffer buffer);
    KryoBuffer getKryoBuffer();
}
