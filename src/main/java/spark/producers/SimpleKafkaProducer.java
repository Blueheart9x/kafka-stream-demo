package spark.producers;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;


import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.RecordMetadata;
import spark.beans.Message;
import spark.beans.StringField;


/**
 * Created by diendn on 6/16/17.
 */
public class SimpleKafkaProducer {
    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteBufferSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int k = 0;

        Message message = new Message();
        message.setAccountId(1);
        message.setDbId(1);
        message.setOperationSource(1);
        message.setStreamId(1);
        message.setAttributeNameField(new StringField("att", k + ""));

        while (true && k++ < 15) {
            ProducerRecord p1 = new ProducerRecord<>("streaming-13", k%4 + "",  message.toByteBuffer());
            Future<RecordMetadata> send = producer.send(p1);
            RecordMetadata recordMetadata = send.get();
            System.out.println(String.format("Topic - %s; Partition - %s", recordMetadata.topic(), recordMetadata.partition()));
            Thread.sleep(10000);

        }


//        producer.close();
    }
}
