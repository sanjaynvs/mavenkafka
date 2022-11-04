package com.sanjay.kafkatest2;

import com.github.redouane59.twitter.IAPIEventListener;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.stream.StreamRules;
import com.github.redouane59.twitter.dto.tweet.Tweet;
import com.github.redouane59.twitter.dto.user.User;
//import com.github.redouane59.twitter.helpers.TweetStreamConsumer;
import com.github.redouane59.twitter.signature.TwitterCredentials;
//import com.github.redouane59.twitter.IAPIEventListener;
import com.github.scribejava.core.model.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class TwitteredClient extends TimerTask {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(TwitteredClient.class.getName());

        TwitterClient twitterClient = new TwitterClient(TwitterCredentials.builder()
                .accessToken("102343857-PytIbKcqqIug717AKb4VpWwg5zxrzuTf3IaerlfV")
                .accessTokenSecret("0scOjHcD86hLvcoceX4sk2ZWYbCAv0a9MuP4aa8Rdklcu")
                .apiKey("XCgwMJQ68BtTpgMoK8imy7D3x")
                .apiSecretKey("jvE2yqBAXm2Owfw7690PyzLvNFJMeLeWGaecRstL7olBNXxe9c")
                .build());

        List<StreamRules.StreamRule> lsr = twitterClient.retrieveFilteredStreamRules();
        for(StreamRules.StreamRule rule: lsr){
            logger.info("in for loop");
            logger.info("rule id " + rule.getId());
            logger.info("rule tag " + rule.getTag());
            logger.info("rule Value " + rule.getValue());
            logger.info("rule toString " + rule.toString());

        }

        StreamRules.StreamMeta sm = twitterClient.deleteFilteredStreamRule("#cricket");
        logger.info("Post filter deletion, sm.getSummary().toString(): " + sm.getSummary().toString());

        StreamRules.StreamRule streamRuleCricket = twitterClient.addFilteredStreamRule("#cricket", "india");
        logger.info("Post filter addition (cricket), sm.getSummary().toString(): " + streamRuleCricket.toString());


//        StreamRules.StreamRule streamRuleFootBall = twitterClient.addFilteredStreamRule("#football", "germany");
//        logger.info("Post filter addition, sm.getSummary().toString(): " + streamRuleFootBall.toString());
    Future<Response> res = twitterClient.startFilteredStream(new IAPIEventListener() {
                @Override
                public void onStreamError(int i, String s) {

                    logger.error("i...."+i);
//                    System.out.println("i...."+i);
//                    System.out.println("s...."+s);
                    logger.error("s...."+s);
                }

                @Override
                public void onTweetStreamed(Tweet tweet) {
                    //counter ++;
                    logger.info("[mention] from:@" + tweet.getUser().getName() + " : " + tweet.getText());

                }

                @Override
                public void onUnknownDataStreamed(String s) {

                }

                @Override
                public void onStreamEnded(Exception e) {
                    System.out.println("ended...");

                }
            });


//        twitterClient.stopFilteredStream();


    }

    @Override
    public void run() {



    }
}
