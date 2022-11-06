package com.sanjay.kafkatest2;

import com.github.redouane59.twitter.IAPIEventListener;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.tweet.Tweet;
import com.github.redouane59.twitter.signature.TwitterCredentials;
import com.github.scribejava.core.model.Response;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;

public class TwitterProducerTimed{
    private Timer timer;
    private TwitterStream ts;
    private Logger logger = LoggerFactory.getLogger(TwitterProducerTimed.class.getName());
    private TwitterClient twitterClient;
    private Future<Response> res;
    private Properties properties = new Properties();
    private KafkaStreamProducer ksp;

//    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
//    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
//    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


    public void TwitterClientStart(int seconds){
        logger.info(" in the method TwitterClient Start");

        ts = new TwitterStream();
        ts.start();
        logger.info("TwitterStream started...");
        timer = new Timer();
        timer.schedule(new RemindTask(), seconds*1000);
        logger.info("Started Timer...");
        logger.info("Start time..."+System.currentTimeMillis());

    }

    public void TwitterClientStop(){
        logger.info("int method TwitterClientStop");
        logger.info("End time..."+System.currentTimeMillis());
        logger.info("Stopping the tweet steam...");
        twitterClient.stopFilteredStream(res);
        KafkaProducer<String,String> producer = ksp.getProducer();
        producer.close();
        ts.interrupt();
    }

    class RemindTask extends TimerTask {
        Logger logger = LoggerFactory.getLogger(RemindTask.class.getName());
        public void run() {
            logger.info("in run method");
            logger.info("Invoking TwitterClientStop");
            TwitterClientStop();
            logger.info("Invoking timer.cancel()");
            timer.cancel();
        }

    }

    public static void main(String args[]) {
        Logger mainmethodlogger = LoggerFactory.getLogger(TwitterProducerTimed.class.getName());
        mainmethodlogger.info("Main method....");
        mainmethodlogger.info("Creating TwitterClientTimed and invoking TwitterClientStart method");
        new TwitterProducerTimed().TwitterClientStart(60);
    }

    public class TwitterStream extends Thread{

        private Logger logger = LoggerFactory.getLogger(TwitterStream.class.getName());

        @Override
        public void run() {
            logger.info("Run method of TwitterStream.....");

            logger.info("Creating Kafka Producer.....");
            ksp = new KafkaStreamProducer();
            KafkaProducer<String,String >producer = ksp.getProducer();
            logger.info("Creating Twitter stream.....");
            twitterClient = new TwitterClient(TwitterCredentials.builder()
                    .accessToken("102343857-PytIbKcqqIug717AKb4VpWwg5zxrzuTf3IaerlfV")
                    .accessTokenSecret("0scOjHcD86hLvcoceX4sk2ZWYbCAv0a9MuP4aa8Rdklcu")
                    .apiKey("XCgwMJQ68BtTpgMoK8imy7D3x")
                    .apiSecretKey("jvE2yqBAXm2Owfw7690PyzLvNFJMeLeWGaecRstL7olBNXxe9c")
                    .build());

            logger.info("Starting tweet streams.....");

            res = twitterClient.startFilteredStream(new IAPIEventListener() {
                @Override
                public void onStreamError(int i, String s) {
                    logger.error("i...."+i);
                    logger.error("s...."+s);
                }

                @Override
                public void onTweetStreamed(Tweet tweet) {
                    String tweetString = "[mention] from:@" + tweet.getUser().getName() + " : " + tweet.getText();
                    logger.info(tweetString);
                    logger.info("Sending to Kafka Producer...");
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("tweet_cricket_football",tweetString);
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null){
                                logger.error("Something bad happened...",e);
                            }
                        }
                    });
                    producer.flush();
                }

                @Override
                public void onUnknownDataStreamed(String s) {
                }

                @Override
                public void onStreamEnded(Exception e) {
                    logger.info("Steam ended...");

                }
            });

        }
    }

    public class KafkaStreamProducer{
        private Properties properties;
        private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
//        ProducerRecord<String, String> record;

        public KafkaStreamProducer(){
  //        Create Producer properties
            properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

            //create safe producer
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");//kafka ver > 2.0...
            //...greater than 1.1, so we keep this 5, else needs to be 1

            //high throughput producer(at the expense of bit of latency and CPU usage introduced!)
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));//32 kb batch size

//          Create the producer
            producer = new KafkaProducer<String, String>(properties);

            //try to understand the below settings.
            //max.block.ms
            

        }

        public  KafkaProducer<String,String> getProducer(){
            return producer;
        }


    }

}
