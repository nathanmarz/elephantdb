package elephantdb.hadoop;

import elephantdb.Utils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO: Destroy this class. No more need for it!
 */
public class ElephantRecordWritable implements Writable {
    byte[] key;
    byte[] val;

    public ElephantRecordWritable() {
    }

    public ElephantRecordWritable(byte[] key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    public void write(DataOutput d) throws IOException {
        Utils.writeByteArray(d, key);
        Utils.writeByteArray(d, val);
    }

    public void readFields(DataInput di) throws IOException {
        this.key = Utils.readByteArray(di);
        this.val = Utils.readByteArray(di);
    }

}