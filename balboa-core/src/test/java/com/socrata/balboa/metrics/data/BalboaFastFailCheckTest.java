package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.data.impl.TimeService;
import junit.framework.TestCase;

public class BalboaFastFailCheckTest extends TestCase {

    class MockTimeService extends TimeService {
        long returnTime;
        @Override
        public long currentTimeMillis() {
            return returnTime;
        }
    }

    public void testTimer() throws Exception {
        MockTimeService mockTime = new MockTimeService();
        BalboaFastFailCheck balboaFailer = new BalboaFastFailCheck(mockTime);
        mockTime.returnTime = 1000;
        balboaFailer.markFailure();
        assertFalse(balboaFailer.proceed());
        mockTime.returnTime = 1100;
        assertFalse(balboaFailer.proceed());

        // just past the timeout
        mockTime.returnTime = 1101;
        assertTrue(balboaFailer.proceed());

        // should double the timeout
        balboaFailer.markFailure();
        mockTime.returnTime = 1300;
        assertFalse(balboaFailer.proceed());

        // again, just passed the timeout
        mockTime.returnTime = 1302;
        assertTrue(balboaFailer.proceed());
        balboaFailer.markFailure();
        balboaFailer.markSuccess();
        assertTrue(balboaFailer.proceed());
    }
}