package com.sanjay.kafkatest2;

import com.github.redouane59.twitter.IAPIEventListener;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.stream.StreamRules;
import com.github.redouane59.twitter.dto.tweet.Tweet;
import com.github.redouane59.twitter.dto.user.User;
//import com.github.redouane59.twitter.helpers.TweetStreamConsumer;
import com.github.redouane59.twitter.signature.TwitterCredentials;
//import com.github.redouane59.twitter.IAPIEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwitteredClient {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(TwitteredClient.class.getName());

        TwitterClient twitterClient = new TwitterClient(TwitterCredentials.builder()
                .accessToken("102343857-PytIbKcqqIug717AKb4VpWwg5zxrzuTf3IaerlfV")
                .accessTokenSecret("0scOjHcD86hLvcoceX4sk2ZWYbCAv0a9MuP4aa8Rdklcu")
                .apiKey("XCgwMJQ68BtTpgMoK8imy7D3x")
                .apiSecretKey("jvE2yqBAXm2Owfw7690PyzLvNFJMeLeWGaecRstL7olBNXxe9c")
                .build());

//        User user   = twitterClient.getUserFromUserId("sanny_pj");
//        System.out.println(user.getId());
//        System.out.println(user.getUser().getName());
//        System.out.println(user.getUser().getDisplayedName());
//        System.out.println(user.getUser().getDateOfCreation());
//        System.out.println(user.getUser().getDescription());
//        System.out.println(user.getUser().getTweetCount());
//        System.out.println(user.getUser().getFollowersCount());
//        System.out.println(user.getUser().getFollowingCount());
//        System.out.println(user.getUser().getPinnedTweet());
//        System.out.println(user.getUser().getPinnedTweet());
//        System.out.println(user.getUser().getLocation());
//        System.out.println(user.getUser().getId());
//        System.out.println(user.getUser().getUrl());


//        Tweet tweet   = twitterClient.getTweet("1224041905333379073");
//        System.out.println(tweet.getText());
//        System.out.println(tweet.getCreatedAt());
//        System.out.println(tweet.getLang());
//        System.out.println(tweet.getLikeCount());
//        System.out.println(tweet.getRetweetCount());
//        System.out.println(tweet.getReplyCount());
//        System.out.println(tweet.getUser().getName());
//
//        //twitterClient.startFilteredStream()
//
////        twitterClient.addFilteredStreamRule("#cricket","")
        StreamRules.StreamRule streamRule = twitterClient.addFilteredStreamRule("#cricket", "india");
        //twitterClient.startFilteredStream()
//        twitterClient.

            twitterClient.startFilteredStream(new IAPIEventListener() {
                @Override
                public void onStreamError(int i, String s) {

                    logger.error("i...."+i);
//                    System.out.println("i...."+i);
//                    System.out.println("s...."+s);
                    logger.error("s...."+s);
                }

                @Override
                public void onTweetStreamed(Tweet tweet) {
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



    }
}
