package com.mohamed.kafka.learning.twitterproducer;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

public class BasicConfig {
    public final String OAuthConsumerKey;
    public final String AuthConsumerSecret;
    public final String OAuthAccessToken;
    public final String OAuthAccessTokenSecret;
    public final String kafkaBrokerList;
    public final String searchQuery;
    public final Boolean useMockData;
    public final String mockFilePath;
    public final String kafkaToic;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TrailClass.class);
    public BasicConfig(String YamlFileConfig)  throws FileNotFoundException, IllegalArgumentException
    {
        Yaml yaml = new Yaml();
        File initialFile = new File(YamlFileConfig);
        InputStream inputStream = new FileInputStream(initialFile);
        Map<String, Object> obj = yaml.load(inputStream);
        try {
            String nullValue="EMPTY";
            OAuthConsumerKey = (String) obj.getOrDefault("OAuthConsumerKey",nullValue);
            AuthConsumerSecret = (String) obj.getOrDefault("AuthConsumerSecret",nullValue);
            OAuthAccessToken = (String) obj.getOrDefault("OAuthAccessToken",nullValue);
            OAuthAccessTokenSecret = (String) obj.getOrDefault("OAuthAccessTokenSecret",nullValue);
            kafkaBrokerList = (String) obj.getOrDefault("kafkaBrokerList",nullValue);
            searchQuery = (String) obj.getOrDefault("searchQuery",nullValue);
            useMockData = Boolean.parseBoolean((String) obj.getOrDefault("useMockData","True"));
            mockFilePath=(String) obj.getOrDefault("mockFilePath","./mockData.txt");
            kafkaToic = (String) obj.getOrDefault("kafkaToic","test-56");
            if(OAuthConsumerKey == nullValue || AuthConsumerSecret == nullValue || OAuthAccessToken == nullValue || OAuthAccessTokenSecret==nullValue
            || kafkaBrokerList == nullValue || searchQuery == nullValue)
            {
                logger.error("Could not get some of the keys and some of the keys were null");
                throw new IllegalArgumentException("Could not get some of the keys");
            }
        }
        catch (Exception ex)
        {
            logger.error(ex.getStackTrace().toString());
            throw new IllegalArgumentException("Could not parse file correctly " + YamlFileConfig + ex.getMessage());
        }
    }




}
