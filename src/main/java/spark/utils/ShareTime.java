package spark.utils;

/**
 * Created by diendn on 8/29/17.
 */
public class ShareTime {
    private static Long startTime;

    public static synchronized void setStartTime(Long startTime) {
        ShareTime.startTime = startTime;
    }

    public static synchronized void setEndTime(Long endTime) {
        if(startTime != null)
        System.out.println("Time: " + (endTime - startTime));
    }


}
