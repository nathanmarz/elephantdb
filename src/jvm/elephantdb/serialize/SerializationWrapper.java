package elephantdb.serialize;

/**
 * User: sritchie
 * Date: 1/2/12
 * Time: 12:37 PM
 */
public interface SerializationWrapper {
    void setSerializer(Serializer serializer);
    Serializer getSerializer();
}
