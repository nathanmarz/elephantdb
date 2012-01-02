package elephantdb.serialize;

import java.io.Serializable;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 3:08 PM
 */
public interface Serializer extends Serializable {
    byte[] serialize(Object o);
    Object deserialize(byte[] bytes);
}
