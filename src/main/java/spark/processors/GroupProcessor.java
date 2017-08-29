//package spark.processors;
//
//
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.processor.Processor;
//import org.apache.kafka.streams.processor.ProcessorContext;
//import org.apache.kafka.streams.state.KeyValueIterator;
//import org.apache.kafka.streams.state.KeyValueStore;
//import spark.utils.Constants;
//import spark.utils.ParserUtil;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
///**
// * Created by diendn on 8/16/17.
// */
//public class GroupProcessor implements Processor<String, String> {
//    private ProcessorContext context;
//
//    private KeyValueStore<String, byte[]> pStore;
//    private Long msTimeout;
//    private int batchSize;
//
//    public GroupProcessor(Long msInterval, int batchSize) {
//        this.msTimeout = msInterval;
//        this.batchSize = batchSize;
//    }
//
//    @Override
//    public void init(ProcessorContext context) {
//        this.context = context;
//        pStore = (KeyValueStore) context.getStateStore("Grouped-processor-store-8");
//    }
//
//    @Override
//    public void process(String key, String value) {
//        System.out.println("Group-process");
//        HashMap<String, TimeValue> kvMap;
//        try{
//            kvMap = ParserUtil.byteArr2Map(pStore.get(Constants.KEY));
//        }catch (Exception e){
//            kvMap = new HashMap<>();
//        }
//
//        TimeValue timeValue = kvMap.get(key);
//
//        if (timeValue == null) {
//            timeValue = new TimeValue(System.currentTimeMillis(), value);
//
//        } else {
//
//            timeValue.setTime(System.currentTimeMillis());
//            timeValue.setValue(timeValue.getValue() + Constants.SEPERATOR + value);
//        }
//        kvMap.put(key, timeValue);
//        pStore.put(Constants.KEY, ParserUtil.map2ByteArr(kvMap));
//        forward();
//
//    }
//
//    @Override
//    public void punctuate(long timestamp) {
//
//
//    }
//
//    @Override
//    public void close() {
//
//        pStore.close();
////        ktStore.close();
//    }
//
//
//    private void forward() {
//        HashMap<String, TimeValue> kvMap = ParserUtil.byteArr2Map(pStore.get(Constants.KEY));
//        Iterator<Map.Entry<String, TimeValue>> it = kvMap.entrySet().iterator();
//        while (it.hasNext()) {
//            Map.Entry<String, TimeValue> entry = it.next();
//            String key = entry.getKey();
//            Long time = entry.getValue().getTime();
//            String values = entry.getValue().getValue();
//            System.out.println("Group-forward-key: " +key);
//            System.out.println("Group-forward-count: " +ParserUtil.countMessage(values));
//            System.out.println("..............");
//            if (ParserUtil.countMessage(values) >= batchSize || System.currentTimeMillis() - time >= msTimeout) {
//                context.forward(key, values, "NORMALIZE-processor");
//                kvMap.remove(key);
//                pStore.put(Constants.KEY, ParserUtil.map2ByteArr(kvMap));
//            }
//
//
//        }
//
//    }
//}
