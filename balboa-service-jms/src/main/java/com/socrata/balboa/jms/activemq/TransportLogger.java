package com.socrata.balboa.jms.activemq;

import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TransportLogger implements TransportListener {
    private static Logger log = LoggerFactory.getLogger(TransportLogger.class);

    @Override
    public void onCommand(Object o) {
    }

    @Override
    public void onException(IOException e) {
        log.error("There was an exception on the ConsumerPool's transport.", e);
    }

    @Override
    public void transportInterupted() {
        log.error("ActiveMQ transport interrupted.");
    }

    @Override
    public void transportResumed() {
        log.info("ActiveMQ transport resumed.");
    }
}
