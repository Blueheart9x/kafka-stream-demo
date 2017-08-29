package spark.processors;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.json.simple.parser.ParseException;
import spark.beans.ApiMessage;
import spark.utils.Constants;
import spark.utils.ParserUtil;
import spark.utils.ProducerUtil;

/**
 * Created by diendn on 8/16/17.
 */
public class NormalizeProcessor implements Processor<String, String> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        ApiMessage message = null;
//        try {
            String dbId = key.split(Constants.SEPERATOR)[0];
//            message = ParserUtil.buildMessage(key, value);

            ProducerUtil.publishMessages("streaming-messages-3", key, value);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }


    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
