package spark.rest;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.codehaus.jackson.map.ObjectMapper;
import spark.utils.GlobalAppState;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REST interface for fetching Kafka Stream processing state. Exposes endpoints
 * for local as well as remote client
 */
@Path("kt-store")
public final class MetricsResource {

    private final String ktstoreName;

    public MetricsResource() {
        ktstoreName = "kt-store-5";
    }

    private static final Logger LOGGER = Logger.getLogger(MetricsResource.class.getName());

    /**
     * Local interface for fetching metrics
     *
     * @return Metrics from local and other remote stores (if needed)
     * @throws Exception
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public Response all_metrics() throws Exception {
        Response response = null;
        try {
            Map metrics = getLocalMetrics();
            LOGGER.log(Level.INFO, "Complete store state {0}", metrics);
            String json = new ObjectMapper().writeValueAsString(metrics);
            response = Response.ok().entity(json).build();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error - {0}", e.getMessage());
            e.printStackTrace();
        }
        return response;
    }

    public static Map getLocalMetrics() {
        KafkaStreams ks = GlobalAppState.getInstance().getKafkaStreams();
        Map localMetrics = new HashMap();
        ReadOnlyKeyValueStore<String, Long> ktStore = ks
                .store("kt-store-5",
                        QueryableStoreTypes.<String, Long>keyValueStore());
        LOGGER.log(Level.INFO, "Entries in store {0}", ktStore.approximateNumEntries());
        KeyValueIterator<String, Long> storeIterator = ktStore.all();
        while (storeIterator.hasNext()) {
            KeyValue<String, Long> kv = storeIterator.next();
            localMetrics.put(kv.key, kv.value);

        }
        LOGGER.log(Level.INFO, "Local store state {0}", localMetrics);
        return localMetrics;
    }

}
