package elephantdb.cascading;

import cascading.tuple.Tuple;

/** User: sritchie Date: 12/16/11 Time: 12:12 AM */
public class IdentityGateway implements Gateway<Object> {

    public Object fromTuple(Tuple tuple) {
        return tuple.getObject(1);
    }

    public Tuple toTuple(Object obj) {
        return new Tuple(obj);
    }
}