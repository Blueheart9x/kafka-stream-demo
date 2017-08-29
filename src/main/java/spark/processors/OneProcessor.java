package spark.processors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import spark.beans.Message;
import spark.utils.Constants;
import spark.utils.ParserUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by diendn on 8/17/17.
 */
public class OneProcessor implements Processor<String, ByteBuffer> {

    private ProcessorContext context;

    private KeyValueStore<String, String> kvStore;
    private KeyValueStore<String, Long> ktStore;

    private Long msTimeout;
    private int batchSize;

    public OneProcessor(Long msTimeout, int batchSize) {
        this.msTimeout = msTimeout;
        this.batchSize = batchSize;
    }


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        System.out.println(context.taskId());
        kvStore = (KeyValueStore) context.getStateStore("kv-store-5");
        ktStore = (KeyValueStore) context.getStateStore("kt-store-5");
        context.schedule(1000);


        printCurTime("init");


    }

    @Override
    public void process(String key, ByteBuffer value) {
        printCurTime("process");
        KeyValueIterator<String, Long> iterator = ktStore.all();
        if (iterator.hasNext()) {

        }
        try {
            String k = ParserUtil.getMessageKey(Message.fromByteBuffer(value));
            String v = ParserUtil.getMessageBody(Message.fromByteBuffer(value)).toJSONString();
            String values = kvStore.get(k);

            if (values == null) {
                values = v;
            } else {
                values += (Constants.SEPERATOR + v);
            }
            forward(k, values);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void punctuate(long timestamp) {


    }

    @Override
    public void close() {
    }

    private void forward(String key, String value) {
        if (ParserUtil.countMessage(kvStore.get(key)) >= batchSize) {

            context.forward(key, value, "NORMALIZE-processor");
            ktStore.put(key, 0L);
            kvStore.put(key, "");
        } else {
            ktStore.put(key, System.currentTimeMillis());
            kvStore.put(key, value);
        }
    }


    private void printCurTime(String s) {
        System.out.println(s + " - time: " + System.currentTimeMillis());
    }

}
