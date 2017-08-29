package spark.utils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import spark.beans.ApiMessage;
import spark.beans.Message;
import spark.beans.StringField;

import javax.json.Json;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by diendn on 8/24/17.
 */
public class ParserUtilTest {
    @Test
    public void combine() throws Exception {
        Message message = new Message();
        message.setAccountId(34);
        message.setDbId(235);
        message.setOperationSource(52);
        message.setStreamId(7);
        message.setAttributeNameField(new StringField("att_name", "iPhone"));


        Message message2 = new Message();
        message2.setAccountId(34);
        message2.setDbId(235);
        message2.setOperationSource(52);
        message2.setStreamId(7);
        message2.setAttributeNameField(new StringField("att_name", "Samsung"));

        ByteBuffer b1 = ParserUtil.message2BB(message);
        ByteBuffer b2 = ParserUtil.message2BB(message2);
    }

    @Test
    public void subString() throws Exception {
    }

    @Test
    public void getMessageBody() throws Exception {

        Message message = new Message();
        message.setAccountId(34);
        message.setDbId(235);
        message.setOperationSource(52);
        message.setStreamId(7);
        message.setAttributeNameField(new StringField("att_name", "iPhone"));

        JSONObject expected = new JSONObject();
        JSONObject operator = new JSONObject();
        operator.put("streamId", "7");
        expected.put("_operationSource", "52");
        expected.put("_operator", operator);
        expected.put("att_name", "iPhone");



        JSONObject result = ParserUtil.getMessageBody(message);
        System.out.println(result.toJSONString());
        assertEquals(expected.toJSONString(), result.toJSONString());

    }

    @Test
    public void buildMessage() throws Exception {

        String key = "235" + Constants.SEPERATOR + "34";
        String value = "{\"_operationSource\":52,\"_operator\":{\"streamId\":7},\"att_name\":\"iPhone\"}----{\"_operationSource\":52,\"_operator\":{\"streamId\":7},\"att_name\":\"iPhone\"}----{\"_operationSource\":52,\"_operator\":{\"streamId\":7},\"att_name\":\"iPhone\"}";

        ApiMessage expected = new ApiMessage();
        JSONArray records = new JSONArray();
        records.add(new JSONObject(){{
            JSONObject operator = new JSONObject(){{
                put("streamId", 7);
            }};
            put("_operationSource", 52);

            put("_operator", operator);
            put("att_name", "iPhone");
        }});
        records.add(new JSONObject(){{
            JSONObject operator = new JSONObject(){{
                put("streamId", 7);
            }};
            put("_operationSource", 52);

            put("_operator", operator);
            put("att_name", "iPhone");
        }});
        records.add(new JSONObject(){{
            JSONObject operator = new JSONObject(){{
                put("streamId", 7);
            }};
            put("_operationSource", 52);

            put("_operator", operator);
            put("att_name", "iPhone");
        }});

        expected.setAccountId(34L);
        expected.setRecords(records);

        ApiMessage result = ParserUtil.buildMessage(key, value);
        assertEquals(expected.toString(), result.toString());

    }

}