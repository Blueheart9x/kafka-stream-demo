package spark.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;
import spark.processors.MapProcessor;
import spark.processors.NormalizeProcessor;
import spark.processors.OneProcessor;

import java.util.Properties;

/**
 * Created by diendn on 8/16/17.
 */
public class ConsumerWithProcessor {

    public  KafkaStreams startPipeline() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo11");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteBuffer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        Long timeout = 5000L;
        int batchSize = 5;

        TopologyBuilder builder = new TopologyBuilder();
        StateStoreSupplier kvStore =
                Stores.create("kv-store-5")
                        .withStringKeys()
                        .withStringValues()
                        .inMemory()
//                        .persistent()
//                        .enableCaching()
                        .build();
        StateStoreSupplier ktStore =
                Stores.create("kt-store-5")
                        .withStringKeys()
                        .withLongValues()
                        .inMemory()
//                        .persistent()
//                        .enableCaching()
                        .build();


        builder.addSource("messages-source", Serdes.String().deserializer(), Serdes.ByteBuffer().deserializer(),"streaming-13")
                .addProcessor("MAP-processor",
                        () -> new MapProcessor(),
                        "messages-source")
                .addProcessor("GROUP-processor",
                        () -> new OneProcessor(timeout, batchSize),
                        "MAP-processor")
                .addProcessor("NORMALIZE-processor",
                        () -> new NormalizeProcessor(),
                        "GROUP-processor")
//                .addProcessor("ONE-processor",
//                        () -> new OneProcessor(timeout, batchSize),
//                        "messages-source")
                .addStateStore(kvStore, "GROUP-processor")
                .addStateStore(ktStore, "GROUP-processor");



        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        streams.setStateListener((state, state1) -> System.out.println(state+" ==== "+state1));
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        return streams;
    }
}
