package com.sanjay.kafkatest2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

public class TwitterClientTimed{
    private Timer timer;
    private TwitterStream ts;
    private Logger logger = LoggerFactory.getLogger(TwitterClientTimed.class.getName());

    public void TwitterClientStart(int seconds){
        logger.info(" in the method TwitterClient Start");

        ts = new TwitterStream();
        ts.start();
        logger.info("TwitterStream started...");
        timer = new Timer();
        timer.schedule(new RemindTask(), seconds*1000);
        logger.info("Started Timer...");
        logger.info("Start time..."+System.currentTimeMillis());
//        timer.schedule(new RemindTask(), seconds*1000, 5000);

    }

    public void TwitterClientStop(){
        logger.info("int method TwitterClientStop");
        logger.info("End time..."+System.currentTimeMillis());
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
        Logger mainmethodlogger = LoggerFactory.getLogger(TwitterClientTimed.class.getName());
        mainmethodlogger.info("Main method....");
        mainmethodlogger.info("Creating TwitterClientTimed and invoking TwitterClientStart method");
        new TwitterClientTimed().TwitterClientStart(30);

        //logger.info("Task scheduled.");
    }

    public class TwitterStream extends Thread{

        private Logger logger = LoggerFactory.getLogger(TwitterStream.class.getName());
        @Override
        public void run() {
            logger.info("Run method of TwitterStream.....");
        }
    }



}
