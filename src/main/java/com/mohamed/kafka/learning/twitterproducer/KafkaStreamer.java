package com.mohamed.kafka.learning.twitterproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaStreamer implements IStreamable {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaStreamer.class);
    private final BasicConfig basicConfig;
    private KafkaProducer<String,String> kafkaProducer;
    @Override
    public boolean addMsg(String key, String value)  {
        ProducerRecord<String,String> msg = new ProducerRecord<>(basicConfig.kafkaToic,key,value);
        logger.info("Attempting to send to Kafka");
        try {
            RecordMetadata meta = kafkaProducer.send(msg).get();
            logger.info("Successfully sent message to kakfa!");
        }
        catch (InterruptedException ex1)
        {
            logger.error(ex1.getMessage());
            return false;
        }
        catch (ExecutionException ex2)
        {
            logger.error(ex2.getMessage());
            return false;
        }
        kafkaProducer.flush();
       return true;
    }
    public KafkaStreamer(BasicConfig basicConfig)
    {
        this.basicConfig=basicConfig;
        initProducer();
    }
    private void initProducer()
    {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,basicConfig.kafkaBrokerList);

       kafkaProducer = new KafkaProducer<String, String>(kafkaProps);


    }
}
