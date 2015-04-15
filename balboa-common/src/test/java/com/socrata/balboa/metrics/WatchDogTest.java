package com.socrata.balboa.metrics;

import com.socrata.balboa.metrics.data.BalboaFastFailCheck;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WatchDogTest {

    class MockListener implements WatchDog.WatchDogListener {
        int starts = 0, stops = 0, hearts = 0, ensure = 0;

        @Override
        public void onStart() { starts++;}
        @Override
        public void onStop() { stops++; }
        @Override
        public void heartbeat() { hearts++; }
        @Override
        public void ensureStarted() {ensure++;}
    }

    class MockBalboaFailCheck extends BalboaFastFailCheck {
        boolean failure = false;
        boolean proceed = false;
        MockBalboaFailCheck() {
            super(null);
        }

        @Override
        public boolean isInFailureMode() {
           return failure;
        }

        @Override
        public boolean proceed() {
            return proceed;
        }

    }

    @Test
    public void testWatchAndWait() throws Exception {

        WatchDog wd = new WatchDog();
        MockListener l = new MockListener();
        MockBalboaFailCheck fc = new MockBalboaFailCheck();

        fc.failure = true;
        fc.proceed = false;

        // stop on failure
        wd.check(fc, l);
        assertEquals(1, l.stops);

        fc.proceed = true;

        // start when on proceed
        wd.check(fc, l);
        assertEquals(1, l.stops);
        assertEquals(1, l.starts);

        // stop on failure again
        fc.proceed = false;
        wd.check(fc, l);
        assertEquals(2, l.stops);
        assertEquals(1, l.starts);

        // again
        wd.check(fc, l);
        assertEquals(3, l.stops);
        assertEquals(1, l.starts);

        fc.proceed = true;
        fc.failure = false;
        wd.check(fc, l);
        wd.check(fc, l);

        assertEquals(2, l.ensure);

        // check heartbeats
        assertEquals(6, l.hearts);
    }
}
