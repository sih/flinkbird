package eu.waldonia.labs.flinkbird.examples;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author sih
 */
public class ManualStatusStreamConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManualStatusStreamConsumer.class);

    private BasicClient hosebirdClient;

    // TODO remove me and replace with Flink class
    private BlockingQueue<String> queue;


    /**
     * @param consumerKey The Twitter Developer program consumer key
     * @param consumerSecret The Twitter Developer porgam consumer secret
     * @param token API token for Twitter app
     * @param secret API secret for Twitter app
     */
    public ManualStatusStreamConsumer(String consumerKey, String consumerSecret, String token, String secret) {
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        // Create an appropriately sized blocking queue
        queue = new LinkedBlockingQueue<String>(10000);

        // Create a new BasicClient. By default gzip is enabled.
        hosebirdClient = new ClientBuilder()
                .name("FlinkbirdClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                // TODO create a new processor
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }

    public void consume() throws InterruptedException {
        // Establish a connection
        hosebirdClient.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            if (hosebirdClient.isDone()) {
                LOGGER.info("Client connection closed unexpectedly: " + hosebirdClient.getExitEvent().getMessage());
                break;
            }

            String msg = queue.poll(5, TimeUnit.SECONDS);
            if (msg == null) {
                LOGGER.warn("Did not receive a message in 5 seconds");
            } else {
                LOGGER.info(msg);
            }
        }

        hosebirdClient.stop();

        // Print some stats
        LOGGER.info("The client read {} messages!\n", hosebirdClient.getStatsTracker().getNumMessages());

    }


    /**
     * @param args Expects the consumerKey, consumerSecret, token, and secret to be passed
     */
    public static void main(String[] args) {
        try {
            ManualStatusStreamConsumer ssc = new ManualStatusStreamConsumer(args[0], args[1], args[2], args[3]);
            ssc.consume();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
