package com.mohamed.kafka.learning.twitterproducer.mock;

import com.mohamed.kafka.learning.twitterproducer.BasicConfig;
import com.mohamed.kafka.learning.twitterproducer.EntryPoint;
import org.apache.kafka.common.metrics.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class MockQueryResult implements QueryResult {
    private static final Logger logger = LoggerFactory.getLogger(EntryPoint.class);
    private final String searchQuery;
    private int dataCount=10;
    private List<String> mockStatueses;
    private final String mockFileLocation;
    private final int readDataSize;
    private int currentIndex;

    public MockQueryResult(String searchQuery, BasicConfig basicConfig) throws FileNotFoundException
    {
        this.searchQuery=searchQuery;
        mockFileLocation=basicConfig.mockFilePath;
        mockStatueses = new ArrayList<>();
        fillStatuses();
        readDataSize=mockStatueses.size();
    }
    private void fillStatuses() throws FileNotFoundException
    {
        logger.info("In fill statuses");
        File file = new File(mockFileLocation);
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine()) {
            String readLine = sc.nextLine();
            logger.info("Read line from mock file is {}",readLine);
           mockStatueses.add(readLine);
        }
        logger.info("Finbished parsing the mock file");
    }

    @Override
    public long getSinceId() {
        return 0;
    }

    @Override
    public long getMaxId() {
        return 0;
    }

    @Override
    public String getRefreshURL() {
        return null;
    }

    @Override
    public int getCount() {
        return this.dataCount;
    }

    @Override
    public double getCompletedIn() {
        return 100;
    }

    @Override
    public String getQuery() {
        return searchQuery;
    }

    @Override
    public List<Status> getTweets() {
        List<Status> returnedStatues = new ArrayList<>(dataCount);
        for (int i=0;i<dataCount;i++)
        {
            int lookupIndex = (i + currentIndex) % readDataSize; // We should not go over the read size, implement a circular array afterwards
            Status status = new MockStatus(this.mockStatueses.get(lookupIndex));
            returnedStatues.add(status);
        }
        currentIndex+=dataCount;
        return returnedStatues;
    }

    @Override
    public Query nextQuery() {
        Query testQuery = new Query();

        return testQuery;
    }

    @Override
    public boolean hasNext() {
        // Always has data
        return true;
    }

    @Override
    public RateLimitStatus getRateLimitStatus() {
        return null;
    }

    @Override
    public int getAccessLevel() {
        return 0;
    }
}
