package spark.utils;

import org.apache.kafka.streams.kstream.Window;

/**
 * Created by diendn on 8/15/17.
 */
public class Constants {

    public static final String API_TOPIC = "api-messages-input";
    public static final String FILTER_TOPIC = "messages-filter";
    public static final String SEPERATOR = "----";
    public static final int BATCH_SIZE = 500;

    public static String KEY = "nal";

    public static String INSERT_BULK_ENDPOINT = "http://localhost:8080/sys/dbs/db_id/records/bulk";


}
