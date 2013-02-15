package elephantdb.cascading;

import cascading.tuple.Tuple;

import java.io.Serializable;

/** User: sritchie Date: 12/16/11 Time: 12:04 AM */
public interface Gateway<D> extends Serializable {
    public D fromTuple(Tuple tuple);
    public Tuple toTuple(D document);
}