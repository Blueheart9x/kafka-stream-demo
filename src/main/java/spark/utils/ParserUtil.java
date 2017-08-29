package spark.utils;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import spark.beans.ApiMessage;
import spark.beans.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by diendn on 8/14/17.
 */
public class ParserUtil {
    public static String bb2Str(ByteBuffer k) {
        return new String(k.array(), Charset.forName("UTF-8"));
    }

    public static ByteBuffer str2BB(String k) {
        return ByteBuffer.wrap(k.getBytes());
    }


    public static JSONObject getMessageBody(Message m) {
        JSONObject record = new JSONObject();
        JSONObject operator = new JSONObject();
        operator.put("streamId", m.getStreamId().toString());
        record.put("_operationSource", m.getOperationSource().toString());
        record.put("_operator", operator);

        try {
            record.put(m.getAttributeNameField().getName(),  m.getAttributeNameField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getAttributeValueField().getName(), m.getAttributeValueField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getAverageField().getName(), m.getAverageField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getCategoryField().getName(), m.getCategoryField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getCountField().getName(), m.getCountField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getDatetimeField().getName(), m.getDatetimeField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getMaxField().getName(), m.getMaxField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getMinField().getName(), m.getMinField().getValue().toString());
        } catch (Exception e) {

        }
        try {
            record.put(m.getMeasureField().getName(), m.getMeasureField().getValue().toString());
        } catch (Exception e) {

        }
        return record;
    }


    public static String getMessageKey(Message m) {
        return m.getDbId() + Constants.SEPERATOR + m.getAccountId();
    }


    public static int countMessage(String messages) {
//        System.out.println("Count messages: " +messages);
        if (messages == null) {
            return 0;
        }
        String[] valueArr = messages.split(Constants.SEPERATOR);
        return valueArr.length;
    }


    public static ApiMessage buildMessage(String key, String value) throws ParseException {
        JSONParser parser = new JSONParser();

        ApiMessage message = new ApiMessage();
        JSONArray records = new JSONArray();

        String dbId = key.split(Constants.SEPERATOR)[0];
        Long accountId = Long.valueOf(key.split(Constants.SEPERATOR)[1]);
        String[] recordArr = value.split(Constants.SEPERATOR);
        for (String record : recordArr) {
            records.add((JSONObject) parser.parse(record));
        }

        message.setAccountId(accountId);
        message.setRecords(records);
        return message;

    }


    public static ByteBuffer combine(ByteBuffer v1, ByteBuffer v2) {
        if(ParserUtil.bb2Str(v1) == null || ParserUtil.bb2Str(v1).equals("")){
            return v2;
        }
        return ParserUtil.str2BB(ParserUtil.bb2Str(v1) + Constants.SEPERATOR + ParserUtil.bb2Str(v2));

    }

    public static String subString(String str1, ByteBuffer str2) {
        String str = ParserUtil.bb2Str(str2);
        if (str1 == null) {
            return str;
        }
        return str.substring(str1.length() + Constants.SEPERATOR.length(), str.length());
    }

    public static ByteBuffer message2BB(Message m) throws IOException {
        return ParserUtil.str2BB(ParserUtil.getMessageBody(Message.fromByteBuffer(m.toByteBuffer())).toJSONString());
    }


}
