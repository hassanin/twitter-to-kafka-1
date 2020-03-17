package com.mohamed.kafka.learning.twitterproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class EntryPoint {
    private static final Logger logger = LoggerFactory.getLogger(EntryPoint.class);
    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
        logger.info("Starting ... on Thread ID {}",Thread.currentThread().getId());
        String mainConfigYaml="";
        if(args.length > 0)
        {
            mainConfigYaml=args[0];
            logger.info("Main Config file passed as {}",mainConfigYaml);
        }
        String dir = System.getProperty("user.dir");
        logger.info("Working Directory is {}",dir);
//        try {
            BasicConfig basicConfig = new BasicConfig(mainConfigYaml);
            logger.info(basicConfig.AuthConsumerSecret);
            TwitterConsumer tw = new TwitterConsumer(basicConfig);
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//
//        }));

//        Scanner scanner = new Scanner(System.in);
//        String name = scanner.nextLine();
        Thread.sleep(5000);
        logger.info("Exiting main method");
        /** Region for shutting down the application*/
        logger.info("Starting shutdown hook on thread {}", Thread.currentThread().getId());
        logger.info("Shutting Down the Java Application");
        boolean shutDownResult = tw.shutDownConnector();
        if(shutDownResult==true)
        {
            logger.info("Gracefully shutdown the twitter instance");
        }
        else
        {
            logger.warn("Forced shutdown some of the running twitter instances");
        }
        logger.info("Good Bye...... :)");
//        System.exit(0);
        /**End Rdsegion*/

//            System.in.read();
//        }
//        catch (Exception ex)
//        {
//            logger.error("Caught error {}",ex.getMessage());
//            logger.error(ex.getStackTrace().toString());
//        }
        //Thread.currentThread().setContextClassLoader(null);

    }


}
