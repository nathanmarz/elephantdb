package elephantdb;

import java.util.Arrays;

/**
 * User: sritchie
 * Date: 1/5/12
 * Time: 2:08 PM
 */
public class ByteArray {
    byte[] arr;

    public ByteArray() {
        this(new byte[0]);
    }
    public ByteArray(byte[] b) {
        this.arr = b;
    }

    public byte[] get() {
        return arr;
    }

    @Override
    public boolean equals(Object other) {
        return Arrays.equals(arr, ((ByteArray) other).arr);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(arr);
    }
}