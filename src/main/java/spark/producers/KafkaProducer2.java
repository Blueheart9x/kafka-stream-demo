package spark.producers;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;


import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import spark.beans.DoubleField;
import spark.beans.Message;
import spark.beans.StringField;
import spark.utils.Constants;


/**
 * Created by diendn on 6/16/17.
 */
public class KafkaProducer2 {
    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteBufferSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Message message = new Message();
        message.setAccountId(34);
        message.setDbId(235);
        message.setOperationSource(52);
        message.setStreamId(7);
        message.setAttributeNameField(new StringField("attr_name", "iPhone"));
//        message.setMinField(new DoubleField("min", 15.000));
//        message.setMeasureField(new StringField("measure", "temperature"));
//
        int i = 0;
        while (i++ < 1) {
            int k = 0;
            while (k++ <= 10) {
            ProducerRecord p1 = new ProducerRecord<>("streaming-messages-output-3", message.getDbId() + Constants.SEPERATOR + message.getAccountId(), message.toByteBuffer());
//                ProducerRecord p2 = new ProducerRecord<>("streaming-input-messages", "key-1", "value-2");
//                ProducerRecord p3 = new ProducerRecord<>("streaming-messages-x", "key-3", "value-3");
//                ProducerRecord p4 = new ProducerRecord<>("streaming-messages-x", "key-4", "value-4");
            System.out.println(producer.send(p1));
//                producer.send(p2);

//            System.out.println(producer.send(p3));
//            System.out.println(producer.send(p4));
//                Thread.sleep(20);
//
            }
            System.out.println(i);
            Thread.sleep(1000);
        }


        producer.close();
    }
}
