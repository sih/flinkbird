package eu.waldonia.labs.flinkbird.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author sih
 */
public class NormalizerFunction implements MapFunction<String, JsonNode> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public JsonNode map(String value) throws Exception {
        return mapper.readTree(value);
    }
}
