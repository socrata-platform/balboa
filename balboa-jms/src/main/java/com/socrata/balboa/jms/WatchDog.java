package com.socrata.balboa.jms;

import com.socrata.balboa.metrics.data.BalboaFastFailCheck;

/**
 * Loops Endlessly. Making sure things are cool. Until they are
 * not.
 */
public class WatchDog {
    public void watchAndWait(ActiveMQReceiver receiver) throws InterruptedException {
        BalboaFastFailCheck failCheck = BalboaFastFailCheck.getInstance();
        while(true)
        {
            // Restart ActiveMQ if we are tentatively considering exiting a known
            // back-off failure mode.
            if (failCheck.isInFailureMode()) {
                if (failCheck.proceed()) {
                    if (receiver.isStopped())
                        receiver.restart();
                } else {
                    if (!receiver.isStopped())
                        receiver.stop();
                }
            }
            Thread.sleep(100);
        }
    }

}
