package eu.waldonia.labs.flinkbird.functions;

import com.fasterxml.jackson.databind.JsonNode;
import eu.waldonia.labs.flinkbird.TweetTags;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author sih
 */
public class TextractorFunction implements MapFunction<JsonNode, String>, TweetTags {


    @Override
    public String map(JsonNode tweet) throws Exception {
        // TODO - should really have a filter before this
        return tweet.get(TEXT_ELEMENT).asText();
    }
}
