package eu.waldonia.labs.flinkbird.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @author sih
 */
public class TimeBucketWordCountFunction implements MapFunction<String, Tuple2<Long,Integer>> {

    private String windowUnit;
    private long chunkVal;

    public static final String MINUTES = "M";
    public static final String SECONDS = "S";
    public static final String MILLIS = "MS";
    public static final String HOURS = "H";

    public TimeBucketWordCountFunction() {
        this(SECONDS,10);
    }

    public TimeBucketWordCountFunction(String windowUnit, long chunkVal) {
        this.windowUnit = windowUnit;
        this.chunkVal = chunkVal;
    }


    @Override
    public Tuple2<Long,Integer> map(String value) throws Exception {

        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime midnight = now.truncatedTo(ChronoUnit.DAYS);
        Duration duration = Duration.between(midnight, now);
        long unitsPassed = 0L;

        if (windowUnit.equals(TimeBucketWordCountFunction.SECONDS)) {
            unitsPassed = duration.getSeconds();
        } else if (windowUnit.equals(TimeBucketWordCountFunction.MILLIS)) {
            unitsPassed = duration.get(ChronoUnit.MILLIS);
        } else if (windowUnit.equals(TimeBucketWordCountFunction.HOURS)) {
            unitsPassed = duration.get(ChronoUnit.HOURS);
        } else if (windowUnit.equals(TimeBucketWordCountFunction.MINUTES)) {
            unitsPassed = duration.get(ChronoUnit.MINUTES);
        }

        long bucket = unitsPassed/this.chunkVal;


        return new Tuple2(bucket,1);
    }
}
