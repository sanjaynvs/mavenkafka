package com.sanjay.kafkatest2;



import java.util.HashSet;
import java.util.Set;
import com.twitter.clientlib.TwitterCredentialsOAuth2;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerV2 {

    Logger logger = LoggerFactory.getLogger(TwitterProducerV2.class.getName());
    String consumerKey="XCgwMJQ68BtTpgMoK8imy7D3x";
    String consumerSecret="jvE2yqBAXm2Owfw7690PyzLvNFJMeLeWGaecRstL7olBNXxe9c";
    String token="102343857-PytIbKcqqIug717AKb4VpWwg5zxrzuTf3IaerlfV";
    String secret="0scOjHcD86hLvcoceX4sk2ZWYbCAv0a9MuP4aa8Rdklcu";

    public TwitterProducerV2(){}

    public static void main(String[] args) {
        new TwitterProducerV2().run();
    }

    public void run(){

        logger.info("Setup");

        //create a twitter client
        TwitterApi client = createTwitterClient();
        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");

        try{
            // findTweetById
            Get2TweetsIdResponse result = client.tweets().findTweetById("20")
                    .tweetFields(tweetFields)
                    .execute();
            if(result.getErrors() != null && result.getErrors().size() > 0) {
                System.out.println("Error:");

                result.getErrors().forEach(e -> {
                    System.out.println(e.toString());
                    if (e instanceof ResourceUnauthorizedProblem) {
                        System.out.println(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
                    }
                });
            } else {
                System.out.println("findTweetById - Tweet Text: " + result.toString());
            }
        }catch(ApiException e){
            e.printStackTrace();
        }
        logger.info("End of application");

        //loop to send tweets to kafka
    }

    public TwitterApi createTwitterClient(){

        /**
         * Set the credentials for the required APIs.
         * The Java SDK supports TwitterCredentialsOAuth2 & TwitterCredentialsBearer.
         * Check the 'security' tag of the required APIs in https://api.twitter.com/2/openapi.json in order
         * to use the right credential object.
         */
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsOAuth2(
                System.getenv("XCgwMJQ68BtTpgMoK8imy7D3x"),
                System.getenv("jvE2yqBAXm2Owfw7690PyzLvNFJMeLeWGaecRstL7olBNXxe9c"),
                System.getenv("102343857-PytIbKcqqIug717AKb4VpWwg5zxrzuTf3IaerlfV"),
                System.getenv("0scOjHcD86hLvcoceX4sk2ZWYbCAv0a9MuP4aa8Rdklcu")));

        return apiInstance;


    }
}

