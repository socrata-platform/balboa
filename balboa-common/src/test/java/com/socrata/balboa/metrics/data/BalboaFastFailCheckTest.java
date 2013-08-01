package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.data.impl.TimeService;
import junit.framework.TestCase;

import java.io.IOException;

public class BalboaFastFailCheckTest extends TestCase {

    class MockTimeService extends TimeService {
        long returnTime;
        @Override
        public long currentTimeMillis() {
            return returnTime;
        }
    }

    class SillyException extends IOException {

    }

    public void testTimer() throws Exception {
        MockTimeService mockTime = new MockTimeService();
        BalboaFastFailCheck balboaFailer = new BalboaFastFailCheck(mockTime);
        mockTime.returnTime = 1000;
        balboaFailer.markFailure(new SillyException());
        assertFalse(balboaFailer.proceed());

        try {
            balboaFailer.proceedOrThrow();
            fail("Expected to throw");
        } catch (IOException e) {
            // swallow
            assertTrue(e.getCause() instanceof SillyException);
        }

        mockTime.returnTime = 2000;
        assertFalse(balboaFailer.proceed());

        // just past the timeout
        mockTime.returnTime = 2001;
        assertTrue(balboaFailer.proceed());

        // should double the timeout
        balboaFailer.markFailure(new SillyException());
        mockTime.returnTime = 4000;
        assertFalse(balboaFailer.proceed());

        // again, just passed the timeout
        mockTime.returnTime = 4002;
        assertTrue(balboaFailer.proceed());
        balboaFailer.markFailure(new SillyException());
        balboaFailer.markSuccess();
        assertTrue(balboaFailer.proceed());
    }
}
