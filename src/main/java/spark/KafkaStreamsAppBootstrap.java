package spark;


import org.apache.kafka.streams.KafkaStreams;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.moxy.json.MoxyJsonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import spark.consumers.ConsumerWithProcessor;
import spark.rest.MetricsResource;
import spark.utils.GlobalAppState;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

//import spark.rest.MetricsResource;

/**
 * Start Grizzly services and initiates Kafka stream processing pipeline
 */
public final class KafkaStreamsAppBootstrap {

    private static final Logger LOGGER = Logger.getLogger(KafkaStreamsAppBootstrap.class.getName());
    private final static String HOSTNAME = "0.0.0.0";

    private static void bootstrap() throws IOException {

        Random rnd = new Random();
        String portPart = String.valueOf(rnd.nextInt(10));
        String port = Optional.ofNullable(System.getenv("PORT")).orElse("808" + portPart);

        //Start Grizzly container
        URI baseUri = UriBuilder.fromUri("http://" + HOSTNAME + "/").port(Integer.parseInt(port)).build();
        ResourceConfig config = new ResourceConfig(MetricsResource.class)
                .register(MoxyJsonFeature.class); //to-from JSON (using JAXB)
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
        server.start();
        Logger.getLogger(KafkaStreamsAppBootstrap.class.getName()).log(Level.INFO, "Application accessible at {0}", baseUri.toString());

        GlobalAppState.getInstance()
                .hostPortInfo("localhost", port);


        KafkaStreams theStream = new ConsumerWithProcessor().startPipeline();
        GlobalAppState.getInstance().streams(theStream);





    }

    /**
     * Entry point
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        bootstrap();

    }
}
