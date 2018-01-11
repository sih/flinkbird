# Flinkbird

## Overview
This is a toy project that consumes the Twitter sample stream and uses a set of Flink functions to perform various transformations on the data.

## Setting up
Twitter APIs require authentication, even the sample stream, so to use this demo you'll need to sign up to the [Twitter Developer Program](https://developer.twitter.com/en.html) and register a new application. 

Once that is done then Twitter will give you:
* Consumer Key
* Consumer Key Secret
* Access Token
* Access Token Secret 
        
these are needed to be passed to the main appllication to run.
        

## What does it do?
The application connects to the Twitter sample stream, which streams 1000 random tweets every 5 seconds, and passes this through a set of Flink transformations (functions) to:
1. Normalizes the String response in to a (Jackson) JsonNode
2. Filters the tweets to get only English tweets (or rather where the user's language is stated as English)
3. Extracts the tweet text from the Json object
4. Filters the text to search for a keyword
5. Counts the number of tweets containing this keyword in a time window

````
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
````

## Running the application
The ````FlinkTwitterProcessor```` is the main class of the project and this is where the keys are needed. Build the project using Maven, i.e. ````mvn clean install```` and then from the home directory run the application:

````java -jar ./target/flinkbird-1.0.jar <consumer key> <consumer key secret> <access token> <access token secret>````

This will connect to twitter, consume the stream and start outputting a time bucket number and the count of the keywords. A bucket is a day in the bucket units divided by the bucket size, e.g. for a bucket of 30 secs, the bucket number will be 86400/30 - i.e. the number of seconds in a day divided by 30. An example of the output is below:

````
New connection executed: flink-twitter-source, endpoint: /1.1/statuses/sample.json
Twitter Streaming API connection established successfully
Sending heartbeat to JobManager
flink-twitter-source Establishing a connection
flink-twitter-source Connection successfully established
flink-twitter-source Processing connection data
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
1> (2616,9)
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
2> (2617,14)
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Sending heartbeat to JobManager
3> (2618,17)
Sending heartbeat to JobManager
Sending heartbeat to JobManager
Shutting down I/O manager.
Shutting down BlobCache
Stopped BLOB server at 0.0.0.0:65158
I/O manager removed spill file directory /var/folders/wc/_wnsfz4974n22n6rj1dp76w80000gn/T/flink-io-51a0246e-91c2-49c5-9bf2-ff6cca8ba3ef
````