package eu.waldonia.labs.flinkbird;

import eu.waldonia.labs.flinkbird.functions.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author sih
 */
public class FlinkTwitterProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkTwitterProcessor.class);

    private Properties props;
    private TwitterSource source;
    private StreamExecutionEnvironment env;

    public FlinkTwitterProcessor(
            String consumerKey,
            String consumerSecret,
            String token,
            String tokenSecret,
            StreamExecutionEnvironment see) {

        // set up Twitter authn details
        props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, consumerKey);
        props.setProperty(TwitterSource.CONSUMER_SECRET, consumerSecret);
        props.setProperty(TwitterSource.TOKEN, token);
        props.setProperty(TwitterSource.TOKEN_SECRET, tokenSecret);


        // TODO try out setting a custom endpoint
        source = new TwitterSource(props);

        this.env = (see == null) ? StreamExecutionEnvironment.getExecutionEnvironment() : see;
    }

    public DataStream<String> consume()  {
        DataStream<String> streamSource = env.addSource(source);
        return streamSource;
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, args[0]);
        props.setProperty(TwitterSource.CONSUMER_SECRET, args[1]);
        props.setProperty(TwitterSource.TOKEN, args[2]);
        props.setProperty(TwitterSource.TOKEN_SECRET, args[3]);

        DataStream<String> tweets = env.addSource(new TwitterSource(props));
        tweets
                .map(new NormalizerFunction())
                .filter(new LanguageFilterFunction("en"))
                .map(new TextractorFunction())
                .filter(new KeywordFilterFunction("trump"))
                .map(new TimeBucketWordCountFunction(TimeBucketWordCountFunction.SECONDS,30))
                .timeWindowAll(Time.seconds(30))
                .sum(1)
                .print();
                ;

        env.execute();

    }

}
