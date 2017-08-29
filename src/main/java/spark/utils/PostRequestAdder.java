package spark.utils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * Created by diendn on 8/24/17.
 */
public class PostRequestAdder implements Callable<Integer> {
    String url;
    String jsonData;

    public PostRequestAdder(String url, String jsonData) {
        this.url = url;
        this.jsonData = jsonData;
    }


    public static void main(String[] args) {
        PostRequestAdder adder = new PostRequestAdder("http://localhost:8080/sys/dbs/235/records/bulk", "{\"accountId\":34,\"records\":[{\"operationSourceId\":52,\"_operator\":{\"streamId\":7}},{\"operationSourceId\":52,\"_operator\":{\"streamId\":7}},{\"operationSourceId\":52,\"_operator\":{\"streamId\":7}},{\"operationSourceId\":52,\"_operator\":{\"streamId\":7}},{\"operationSourceId\":52,\"_operator\":{\"streamId\":7}}]}");
        try {
            System.out.println(adder.call());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Integer call() throws Exception {
        return sendPostRequest();
    }

    public Integer sendPostRequest() throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // Setting basic post request
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type","application/json");


        // Send post request
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(jsonData);
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();
//        System.out.println("Sending 'POST' request to URL : " + url);
//        System.out.println("Post Data : " + jsonData);
//        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String output;
        StringBuffer response = new StringBuffer();

        while ((output = in.readLine()) != null) {
            response.append(output);
        }
        in.close();

//        System.out.println(response.toString());
        return (Integer) responseCode;
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getJsonData() {
        return jsonData;
    }

    public void setJsonData(String jsonData) {
        this.jsonData = jsonData;
    }
}
