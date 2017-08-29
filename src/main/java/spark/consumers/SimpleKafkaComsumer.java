package spark.consumers;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.json.simple.parser.ParseException;
import spark.beans.ApiMessage;
import spark.beans.Message;
import spark.test.InsertTest;
import spark.utils.*;
import sun.security.provider.SHA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;


/**
 * Created by diendn on 6/16/17.
 */
public class SimpleKafkaComsumer {
    private static int id;
    private static Map<String, Map<Long, String>> windowedMessagesMap = new Hashtable<>();
    private static ExecutorService executor = Executors.newFixedThreadPool(8);
    static List<Callable<Boolean>> taskLst = new ArrayList<>();
    private static final Logger LOGGER = Logger.getLogger(InsertTest.class.getName());


    public static void main(String[] args) throws IOException {


//    public void startPipe() throws IOException {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo21");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteBuffer().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
//
//        StringSerializer stringSerializer = new StringSerializer();
//        StringDeserializer stringDeserializer = new StringDeserializer();
//        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
//        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
//        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        Long inactivityGapMs = 10000L;


        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, ByteBuffer> records = builder.stream("streaming-messages-output-3");
//        System.out.println("Record: ");
//        records.print();

        records.map((k, v) -> {
            try {
                return new KeyValue<String, ByteBuffer>(k, ParserUtil.message2BB(Message.fromByteBuffer(v)));
            } catch (IOException e) {
                return new KeyValue<String, ByteBuffer>(k, ParserUtil.str2BB(""));
            }
        }).groupByKey()
                .reduce((aggValue, newValue) -> ParserUtil.combine(aggValue, newValue),
                SessionWindows.with(inactivityGapMs))
                .toStream((k, v) -> export(k, v));
//                .to("monitor");


        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        streams.setStateListener((state, state1) -> System.out.println(state + " ==== " + state1));
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static String export(Windowed<String> k, ByteBuffer v) {

        ProducerUtil.publishMessages("monitor", "key", "1");
        LOGGER.info("ID: " +id);
        Long startTime = k.window().start();
        String key = k.key();
        Map<Long, String> timeValues = windowedMessagesMap.get(key);
        if (v != null) {
            String values = null;
            if (timeValues != null) { //key existed
                values = timeValues.get(startTime);  //Get value by start time
                if (values == null) {
                    windowedMessagesMap.remove(key);
                }
            }
            try {
                String dbId = key.split(Constants.SEPERATOR)[0];
                ApiMessage message = ParserUtil.buildMessage(key, ParserUtil.subString(values, v));
                LOGGER.info("Batch size: " + ParserUtil.countMessage(ParserUtil.subString(values, v)));
                Future<Integer> future = executor.submit(new PostRequestAdder(Constants.INSERT_BULK_ENDPOINT.replace("db_id", dbId), message.toString()));
                future.get();
                LOGGER.info("End-Time: " +System.currentTimeMillis());
                ProducerUtil.publishMessages("output", dbId + "@" + k.window().start(), message.toString());
            } catch (ParseException e) {
                e.printStackTrace();

            } catch (Exception e) {
                e.printStackTrace();
            }

            windowedMessagesMap.put(key, new HashMap<Long, String>() {{
                put(startTime, ParserUtil.bb2Str(v));
            }});
        }
        return String.format("Key2 - %s; Value - %s", k.key(), "xxx");
    }


}
