package com.mohamed.kafka.learning.twitterproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class EntryPoint {
    private static final Logger logger = LoggerFactory.getLogger(EntryPoint.class);
    public static void main(String[] args)  {

        logger.info("Starting ...");
        //Thread.currentThread().setContextClassLoader(null);
        try
        {
//            sendTweet();
            List<String> tweets = getTimeLine();
            tweets.forEach(s -> {
                logger.info("Message in time line is {}",s);
            });
            logger.info("Done with main program, now exiting");
        }
        catch (Exception ex)
        {
            logger.error("Caught exception in main twitter app {}",ex.getMessage());
            logger.error(ex.getStackTrace().toString());
        }
    }
    public static void mainKafkaTest() throws ClassNotFoundException
    {
        Properties properties = new Properties();
        String brokers="mhassani-deb9-2:9092";
        String topic="testtopic1";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.toString());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.toString());
        properties.put("key.serializer", Class.forName(StringSerializer.class.getName()));
        properties.put("value.serializer", Class.forName(StringSerializer.class.getName()));
        logger.info(StringSerializer.class.toString());
        KafkaProducer<String,String> kf = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,"mkKey","myValue");
        kf.send(record,(metadata,error) -> {
            if(error == null)
                logger.info("Succsfuly sent message to kafka {} "+ metadata.offset());
            else
                logger.error("Caught exception in sending messsage to Kafka");
        });


        kf.flush();
        kf.close();
    }
    public static void sendTweet() throws TwitterException
    {
        Twitter twitter = getTwitterInstance();
        logger.info("Successfully started the twitter API");
        Status status = twitter.updateStatus("creating baeldung API");
        String result = status.getText();
        logger.info("Received {} from twitter",result);
    }

    public static List<String> getTimeLine() throws TwitterException {
        Twitter twitter = getTwitterInstance();
        return twitter.getHomeTimeline().stream()
                .map(item -> item.getText())
                .collect(Collectors.toList());
    }
    public static Twitter getTwitterInstance()
    {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("jC9uzmcEvl1L8jRXRuQVvoJld")
                .setOAuthConsumerSecret("S5laqFw6eEXLbJc5jx9wVNDes3taO8uVHOtKZxhllh4HjVkmvE")
                .setOAuthAccessToken("2401995602-nabkHDxHOB4zE9NOzoUlvvlyHzxRJF5ZPiAspne")
                .setOAuthAccessTokenSecret("Qhdx5an54Df8U2AVUK9ARxcBoZISsMhh0AmoJ1cJEhHby");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;
    }
}
