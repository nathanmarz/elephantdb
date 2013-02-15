package elephantdb.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import elephantdb.DomainSpec;
import elephantdb.Utils;
import elephantdb.document.KeyValDocument;
import elephantdb.hadoop.ElephantInputFormat;
import elephantdb.hadoop.ElephantOutputFormat;
import elephantdb.hadoop.ElephantRecordWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class ElephantScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    Gateway gateway;

    public ElephantScheme(Fields sourceFields, Fields sinkFields, DomainSpec spec, Gateway gateway) {
        setSourceFields(sourceFields);
        setSinkFields(sinkFields);
        this.gateway = gateway;
    }

    @Override public void sourceConfInit(FlowProcess<JobConf> flowProcess,
                                         Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setInputFormat(ElephantInputFormat.class);
    }

    @Override public void sinkConfInit(FlowProcess<JobConf> flowProcess,
                                       Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputKeyClass(IntWritable.class); // be explicit
        conf.setOutputValueClass(ElephantRecordWritable.class); // be explicit
        conf.setOutputFormat(ElephantOutputFormat.class);
    }

    @Override public void sourcePrepare(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {

        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    @Override public boolean source(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) throws IOException {

        NullWritable key = (NullWritable) sourceCall.getContext()[0];
        ElephantRecordWritable value = (ElephantRecordWritable) sourceCall.getContext()[1];

        boolean result = sourceCall.getInput().next(key, value);

        if (!result)
            return false;

        KeyValDocument doc = new KeyValDocument(value.key, value.value);

        sourceCall.getIncomingEntry().setTuple(gateway.toTuple(doc));
        return true;
    }

    @Override public void sink(FlowProcess<JobConf> flowProcess,
        SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();

        int shard = tuple.getInteger(0);
        Object doc = gateway.fromTuple(tuple);
        KeyValDocument pair = (KeyValDocument) doc;

        sinkCall.getOutput().collect(new IntWritable(shard), new ElephantRecordWritable(pair.key, pair.value));
    }
}
