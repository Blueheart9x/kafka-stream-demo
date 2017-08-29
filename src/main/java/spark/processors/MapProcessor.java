package spark.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.json.simple.JSONObject;
import spark.beans.Message;
import spark.utils.Constants;
import spark.utils.ParserUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by diendn on 8/16/17.
 */
public class MapProcessor implements Processor<String, ByteBuffer> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, ByteBuffer value) {
//        System.out.println("MapProcessor - key: " +key);
        try {
            Message message = Message.fromByteBuffer(value);

            String k = ParserUtil.getMessageKey(message);


            String v = ParserUtil.getMessageBody(message).toJSONString();
            context.forward(Constants.KEY, value, "GROUP-processor");
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
}
