package spark.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import spark.processors.OneProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by diendn on 8/22/17.
 */
public class MessageStream {
    private static final String TOPIC_NAME = "streaming-13";
    private static final String SOURCE_NAME = "source";
    private static final String PROCESSOR_NAME = "one-processor";
    public static final String KVSTORE_NAME = "key-value-store";
    public static final String KTSTORE_NAME = "key-time-store";


    Long timeout = 5000L;
    int batchSize = 5;

    private static final Logger LOGGER = Logger.getLogger(MessageStream.class.getSimpleName());


    public KafkaStreams startPipeline() {

        Map<String, Object> configurations = new HashMap<>();


        String streamsAppServerConfig = "localhost:9093";

        configurations.put(StreamsConfig.APPLICATION_SERVER_CONFIG, streamsAppServerConfig);
        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, "cpu-streamz-7");
        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        configurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteBuffer().getClass().getName());
        configurations.put(StreamsConfig.STATE_DIR_CONFIG, KVSTORE_NAME);
        configurations.put(StreamsConfig.STATE_DIR_CONFIG, KTSTORE_NAME);
        configurations.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");

        StreamsConfig config = new StreamsConfig(configurations);

        TopologyBuilder builder = processingTopologyBuilder();
        KafkaStreams streams = null;
        boolean connected = false;
        int retries = 0;

        do {
            LOGGER.info("Initiating Kafka Streams");
            try {
                streams = new KafkaStreams(builder, config);
                connected = true;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error during Kafka Stream initialization {0} .. retrying", e.getMessage());
                retries++;
            }

        } while (!connected && retries <= 1); //retry

        if (!connected) {
            LOGGER.warning("Unable to initialize Kafka Streams.. exiting");
            System.exit(0);
        }

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.log(Level.SEVERE, "Uncaught exception in Thread {0} - {1}", new Object[]{t, e.getMessage()});
            }
        });

        streams.cleanUp();
        streams.setStateListener((state, state1) -> System.out.println(state+" ==== "+state1));
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;

    }

    private TopologyBuilder processingTopologyBuilder() {

        StateStoreSupplier kvStore
                = Stores.create(KVSTORE_NAME)
                .withStringKeys()
                .withStringValues()
                .inMemory()
                .build();

        StateStoreSupplier ktStore
                = Stores.create(KTSTORE_NAME)
                .withStringKeys()
                .withLongValues()
                .inMemory()
                .build();

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource(SOURCE_NAME, Serdes.String().deserializer(), Serdes.ByteBuffer().deserializer(), TOPIC_NAME)
                .addProcessor(PROCESSOR_NAME,
                        () -> new OneProcessor(timeout, batchSize),
                        SOURCE_NAME)
                .addStateStore(ktStore, PROCESSOR_NAME)
                .addStateStore(kvStore, PROCESSOR_NAME);

        LOGGER.info("Kafka streams processing topology ready");

        return builder;
    }

    public static void main(String[] args) {
        KafkaStreams theStream = new MessageStream().startPipeline();
    }

}
