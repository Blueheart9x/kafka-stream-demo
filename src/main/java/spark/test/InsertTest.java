package spark.test;

import spark.beans.DoubleField;
import spark.beans.IntField;
import spark.beans.Message;
import spark.beans.StringField;
import spark.rest.MetricsResource;
import spark.utils.Constants;
import spark.utils.PostRequestAdder;
import spark.utils.ProducerUtil;
import spark.utils.ShareTime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Created by diendn on 8/29/17.
 */
public class InsertTest {
    private static final Logger LOGGER = Logger.getLogger(InsertTest.class.getName());
    static String ONE_BY_ONE_END_POINT = "http://localhost:8080/sys/dbs/235/records/";
    static String BULK_END_POINT = "http://localhost:8080/sys/dbs/235/records/bulk";
    static int NUM_OF_RECORDS = 1000;
    private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    static String messagesOneByOne = "{\n" +
            "   \"_account\":\"34\",\n" +
            "   \"measure\":\"temperature\",\n" +
            "   \"_operator\":{\n" +
            "      \"streamId\":\"7\"\n" +
            "   },\n" +
            "   \"avg\":\"15.000000000000000\",\n" +
            "   \"attr_name\":\"device_name\",\n" +
            "   \"_operationSource\":\"52\",\n" +
            "   \"min\":\"15.000000000000000\",\n" +
            "   \"attr_value\":\"galaxy\",\n" +
            "   \"interval\":\"40\",\n" +
            "   \"max\":\"15.000000000000000\",\n" +
            "   \"time\":\"2017-07-27 10:20:00+07\"\n" +
            "}";

    public static void main(String[] args) {
//        System.out.println(insertOneByOne());
//        System.out.println(Runtime.getRuntime().availableProcessors());
//        Message message = new Message();
//        message.setAccountId(34);
//        message.setDbId(235);
//        message.setOperationSource(52);
//        message.setStreamId(7);
//        message.setAttributeNameField(new StringField("attr_name", "iPhone"));
//        message.setAverageField(new DoubleField("avg", 15.0));
//        message.setMinField(new DoubleField("min", 10.0));
//        message.setIntervalField(new IntField("interval", 40));
//        message.setMaxField(new DoubleField("max", 20.0));
//        try {
//            insertBulk(message.getDbId() + Constants.SEPERATOR + message.getAccountId(), message.toByteBuffer());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        insertBulk();

//        System.out.println(insertOneByOne());
    }

    public static Long insertOneByOne() {
        Long startMs = System.currentTimeMillis();

        List<Future<Integer>> listFuture = new ArrayList<>();

        for (int i = 0; i < NUM_OF_RECORDS; i++) {
            Future<Integer> future = executor.submit(new PostRequestAdder(ONE_BY_ONE_END_POINT, messagesOneByOne));
            listFuture.add(future);
        }

        for (Future<Integer> future : listFuture) {
            try {
                Integer responseCodr = future.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        Long endMs = System.currentTimeMillis();
        return endMs - startMs;

    }

    public static void insertBulk() { Message message = new Message();
        message.setAccountId(34);
        message.setDbId(235);
        message.setOperationSource(52);
        message.setStreamId(7);
        message.setAttributeNameField(new StringField("attr_name", "iPhone"));
        message.setAverageField(new DoubleField("avg", 15.0));
        message.setMinField(new DoubleField("min", 10.0));
        message.setIntervalField(new IntField("interval", 40));
        message.setMaxField(new DoubleField("max", 20.0));
        String key = message.getDbId() + Constants.SEPERATOR + message.getAccountId();
        ByteBuffer value = null;
        try {
            value = message.toByteBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }

        LOGGER.info("Start-Time: " +System.currentTimeMillis());
        for (int i = 0; i < 100; i++) {
            ProducerUtil.publishBBMessages("streaming-messages-output-4", i%5, key, value);
        }
    }
}
