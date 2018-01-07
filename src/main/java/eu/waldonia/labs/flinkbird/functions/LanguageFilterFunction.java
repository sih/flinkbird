package eu.waldonia.labs.flinkbird.functions;

import com.fasterxml.jackson.databind.JsonNode;
import eu.waldonia.labs.flinkbird.TweetTags;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author sih
 */
public class LanguageFilterFunction implements FilterFunction<JsonNode>, TweetTags {

    private String lang;
    public static final String DEFAULT_LANG_EN = "en";

    public LanguageFilterFunction() {
        this(DEFAULT_LANG_EN);
    }

    public LanguageFilterFunction(String lang) {
        this.lang = lang;
    }

    @Override
    public boolean filter(JsonNode tweet) {
        return (this.isText(tweet) && this.getUserLang(tweet).equals(this.lang));
    }

    private boolean isText(JsonNode tweet) {return tweet.has(TEXT_ELEMENT);}

    private String getUserLang(JsonNode tweet) {
        String lang = null;
        if (tweet.has(USER_ELEMENT)) {
            lang = tweet.get(USER_ELEMENT).get(LANG_ELEMENT).asText();
        }
        return lang;
    }

}
