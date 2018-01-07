package eu.waldonia.labs.flinkbird.functions;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author sih
 */
public class KeywordFilterFunction implements FilterFunction<String> {

    private String keyword;

    public KeywordFilterFunction(String keyword) {
        this.keyword = keyword;
    }

    @Override
    public boolean filter(String tweet) throws Exception {
        return tweet.toLowerCase().contains(keyword.toLowerCase());
    }
}
