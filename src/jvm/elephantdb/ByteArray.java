package elephantdb;

import java.util.Arrays;

/**
 * Really used only for testing inside of Clojure.
 */
public class ByteArray {
    byte[] arr;

    public ByteArray(byte[] b) {
        this.arr = b;
    }

    public byte[] get() {
        return arr;
    }

    @Override public boolean equals(Object other) {
        return Arrays.equals(arr, ((ByteArray) other).arr);
    }

    @Override public int hashCode() {
        return Arrays.hashCode(arr);
    }
}
