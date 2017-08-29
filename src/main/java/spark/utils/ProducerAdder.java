package spark.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Created by diendn on 8/29/17.
 */
public class ProducerAdder implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        return null;
    }

    private static Properties getProperties(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteBufferSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    public static void publishBBMessages(String topic,String key, ByteBuffer messageBody){
        ProducerRecord message = new ProducerRecord<>(topic, key, messageBody);
        Properties props = getProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(message);
    }
}
