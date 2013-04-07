package elephantdb.hadoop;

import elephantdb.Utils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ElephantRecordWritable implements Writable {
    public byte[] key;
    public byte[] value;

    public ElephantRecordWritable() {
    }

    public ElephantRecordWritable(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public void write(DataOutput out) throws IOException {
        Utils.writeByteArray(out, key);
        Utils.writeByteArray(out, value);
    }

    public void readFields(DataInput in) throws IOException {
        this.key = Utils.readByteArray(in);
        this.value = Utils.readByteArray(in);
    }
} 
