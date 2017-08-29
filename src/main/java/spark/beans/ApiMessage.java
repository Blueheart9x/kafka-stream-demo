package spark.beans;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * Created by diendn on 8/15/17.
 */
public class ApiMessage {
    private Long accountId;
    private JSONArray records;

    private Long getAccountId() {
        return accountId;
    }

    public void setAccountId(Long accountId) {
        this.accountId = accountId;
    }


    public void setRecords(JSONArray records) {
        this.records = records;
    }

    public String toString(){
        JSONObject object = new JSONObject();
        object.put("_account", getAccountId());
        object.put("records", records);
        return object.toJSONString();
    }

}
