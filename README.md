# twitter-to-kafka-1
This project streams data from twitter into Apache Kafka. It uses twitter4j API to query twitter and then sends the data to a Kafka Broker.

This project can be used as the base of a larger project that performs realtime tweet analysis (To be Followed).

To run the connector, compile the maven project and pass a YAML input file containing (see ./config/producer-template.yml)

> Twitter: See https://developer.twitter.com/en/apply-for-access to get your own developer API keys

OAuthConsumerKey: "GETFROMTWITTER"

AuthConsumerSecret: "GETFROMTWITTER"

OAuthAccessToken: "GETFROMTWITTER"

OAuthAccessTokenSecret: "GETFROMTWITTER"
> The Kafka Beoker to send tweets to

kafkaBrokerList: "kafkabroker:9092"
> Topic to stream data to

kafkaToic: "test-45"
> The search query to filter tweets on, put * for all tweets (NOT RECOMMENDED)
  
searchQuery: "search-me"
>  if provided : the mockFilePath will read tweets from the file and mock using the Twitter API, this is very useful for testing purposes when Twitter rate limits you

mockFilePath: "./config/mockData.txt"
