package com.mohamed.kafka.learning.twitterproducer;

import com.mohamed.kafka.learning.twitterproducer.mock.MockQueryResult;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TwitterConsumer {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);
    private Thread twtiiterConsumerThread,kafkaProducerThread,diagnosticsThread;
    private volatile Deque<Status> mainDeque;
    private final int maxQueueLength=200;
    private int pauseCounter=1;
    private int backuoDuration=100; // 100 milliseconds
    private final Twitter twitter;
    private final BasicConfig basicConfig;
    private volatile boolean shouldShutdown=false;
    private CountDownLatch AllThreadsLatch = new CountDownLatch(3); // we have Kafka and Twitter
    private final boolean debugMode=true;
    private List<String> diagnosticsStrings = new ArrayList<>();
    public TwitterConsumer(BasicConfig basicConfig)
    {
        this.basicConfig=basicConfig;
        twitter=getTwitterInstance();
        mainDeque = new ConcurrentLinkedDeque<>();
        twtiiterConsumerThread = new Thread(()-> {
            mainTwitterLoop();
        });
        kafkaProducerThread = new Thread(()-> {
            mainKakaLoop();
        });
        diagnosticsThread = new Thread(()->{
            runDiagnosticsLoop();
        });
        twtiiterConsumerThread.start();
        kafkaProducerThread.start();
        diagnosticsThread.start();
    }
    public boolean shutDownConnector()
    {
        return shutDownConnector(4,TimeUnit.SECONDS);
    }
    public boolean shutDownConnector(long timeout,TimeUnit timeUnit)
    {
        this.shouldShutdown=true;
        try {
            boolean result = AllThreadsLatch.await(timeout, timeUnit);
//            kafkaProducerThread.interrupt();
            twtiiterConsumerThread.interrupt();
//            writeDiagnosticsToFile();
            if(result == true) {
                logger.info("All Threads shutdown successfully!!");
                return true;
            }
            else {
                logger.warn("Some Threads could not finish on time , graceful shutdown interrupted");
                return false;
            }
        }
        catch (InterruptedException ex)
        {
            logger.info("Caught Interrupted exception, could not shutdown all threads in the allotted time");
        }
        return  true;
    }
    private void writeDiagnosticsToFile()
    {
        try {
            String fileName = "./output/out.txt";
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            diagnosticsStrings.forEach(s ->{
                try {
                    writer.write(s + " ");
                }
                catch (Exception ex)
                {
                    logger.error("Caught exception wriiting line by line diags to disk {}",ex.getMessage());
                }
            });
            writer.flush();
        }
        catch (Exception ex)
        {
            logger.error("Caught exception when writing diagnostics to file");
        }
    }
    private void runDiagnosticsLoop()
    {
        logger.info("Run Diagnostics running in Thread {}",Thread.currentThread().getId());
        while(!shouldShutdown)
        {
            try {
                diagnosticsStrings.add(" " + mainDeque.size());
                Thread.sleep(50);
            }
            catch (InterruptedException exz)
            {
                logger.error("Caught interrupted exception in runDiagnostics");
            }
        }
        logger.info("Run Diagnostics is closing down");
        writeDiagnosticsToFile();
        AllThreadsLatch.countDown();
    }
    public String getDiagnostics()
    {
        String result="";
        if(mainDeque != null)
        {
            int size = mainDeque.size();
            result="Current data in queue is" + size;
        }
        return result;
    }
    private void mainKakaLoop()
    {
        logger.info("Started main Kakfa Loop thread with Thread ID {}",Thread.currentThread().getId());
        IStreamable producer = new KafkaStreamer(basicConfig);
        while (shouldShutdown != true)
        {

            try {
                logger.info("Kakfa Doing stuff");

                if(mainDeque.size() > 0)
                {
                    Status status = mainDeque.getFirst();
                    boolean result = producer.addMsg(Long.toString(status.getId()),status.getText());
                    if(result == true)
                    {
                        logger.info("added tweet successfully yo Kakfa");
                        mainDeque.removeFirst();
                    }
                }
                else {
                    logger.info("No Data found, sleeping for 1 seconds");
                    Thread.sleep(1000); // implement monitor object
                }
            }
            catch (InterruptedException ex)
            {
                logger.warn("Kafka Thread sleep interrupted!");
            }
        }
        logger.info("Kakfa main loop recieved shutdown command, exiting now");
        AllThreadsLatch.countDown();
    }
    private void mainTwitterLoop()
    {
        logger.info("Started main Twitter Loop thread with Thread ID {}",Thread.currentThread().getId());
            try {
                Query query;
                QueryResult result;
                if(!debugMode)
                {
                    query = new Query(basicConfig.searchQuery);
                }
                else {
                    result = new MockQueryResult(basicConfig.searchQuery,basicConfig);
                }
                do {
                    if(!debugMode) {

                        result = twitter.search(query);
                    }

                    List<Status> tweets = result.getTweets();
                    for (Status tweet : tweets) {
                        if(shouldShutdown != true) {
//                            System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
                            logger.info("Received tweet {}",tweet.getText());
                            boolean success= this.addElement(tweet);
                            if(success)
                            {
                                // check offset here
                            }

                        }
                        else {
                            break;
                        }
                    }
                    if(!debugMode)
                    {
                        if((query = result.nextQuery()) == null)
                        {
                            break;
                        }
                    }
                } while ( shouldShutdown!=true);
            } catch (TwitterException te) {
                te.printStackTrace();
                System.out.println("Failed to search tweets: " + te.getMessage());
            }
            catch (FileNotFoundException ex)
            {
                logger.error("Inable to find file {}",basicConfig.mockFilePath );
                logger.error("Caught ecceptin {} with stack trace {}",ex.getMessage(),ex.getStackTrace().toString());
            }

        logger.info("Received Shutdown command or finished the search result, exiting mainTwitterLoop");
        AllThreadsLatch.countDown();
    }
    private boolean addElement(Status status)
    {
        if(mainDeque.size()> maxQueueLength)
        {
            try {
                if(shouldShutdown == true)
                {
                    logger.info("Add Element cancelled due to shutdown for element {}",status.toString());
                    return false;
                }
                //TODO: We can use a latch to signal that it is ok to add this element
                int sleepTime = (int) Math.pow(2,pauseCounter) * backuoDuration;
                logger.info("Sleeping for {}",sleepTime);
                Thread.sleep(sleepTime);
                pauseCounter++;
            }
            catch (InterruptedException ex)
            {
                logger.warn("Twitter Connector is shutting Down!");
                logger.error(ex.getMessage());
                logger.error(ex.getStackTrace().toString());
            }
        }
        else {
            mainDeque.addLast(status);
            pauseCounter=0;
            return true;
        }
        return true;
    }

    private  void searchTweets(String regex)
    {
        Twitter twitter = getTwitterInstance();
        Query query = new Query(regex);
        QueryResult result = null;
        try {
            result = twitter.search(query);
            for (Status status : result.getTweets()) {
                System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
            }
        } catch (TwitterException e) {
            e.printStackTrace();
        }

    }

    private  void sendTweet() throws TwitterException
    {
        Twitter twitter = getTwitterInstance();
        logger.info("Successfully started the twitter API");
        Status status = twitter.updateStatus("creating baeldung API");
        String result = status.getText();
        logger.info("Received {} from twitter",result);
    }

    private  List<String> getTimeLine() throws TwitterException {
        Twitter twitter = getTwitterInstance();
        return twitter.getHomeTimeline().stream()
                .map(item -> item.getText())
                .collect(Collectors.toList());
    }
    private Twitter getTwitterInstance()
    {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(basicConfig.OAuthConsumerKey)
                .setOAuthConsumerSecret(basicConfig.AuthConsumerSecret)
                .setOAuthAccessToken(basicConfig.OAuthAccessToken)
                .setOAuthAccessTokenSecret(basicConfig.OAuthAccessTokenSecret);
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;
    }
}
