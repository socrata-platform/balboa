package com.socrata.balboa.jms;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jonathan on 2/25/16.
 */
public class BalboaJmsTest {

    @Test
    public void testParseServers() throws Exception {
        String [] args = new String[3];
        args[0] = "5";
        args[1] = null;
        args[2] = "metrics2012";
        args[1] = "failover:(uri1,uri2)?transportOptions&nestedURIOptions";
        List<String> servers = BalboaJms.parseServers(args);
        assertEquals(1, servers.size());
        args[1] = "failover:(uri1,uri2)?transportOptions&nestedURIOptions,failover:(uri3,uri4)?transportOptions&nestedURIOptions";
        servers = BalboaJms.parseServers(args);
        assertEquals(2, servers.size());

        args = new String[4];
        args[0] = "5";
        args[1] = "failover:(uri1,uri2)?transportOptions&nestedURIOptions";
        args[2] = "failover:(uri3,uri4)?transportOptions&nestedURIOptions";
        args[3] = "metrics2012";
        servers = BalboaJms.parseServers(args);
        assertEquals(2, servers.size());
        assertEquals("failover:(uri1,uri2)?transportOptions&nestedURIOptions&soTimeout=15000&soWriteTimeout=15000", servers.get(0));
        assertEquals("failover:(uri3,uri4)?transportOptions&nestedURIOptions&soTimeout=15000&soWriteTimeout=15000", servers.get(1));


        args[1] = "failover:(uri1,uri2)";
        args[2] = "failover:(uri3,uri4)";
        servers = BalboaJms.parseServers(args);
        assertEquals(2, servers.size());
        assertEquals("failover:(uri1,uri2)?soTimeout=15000&soWriteTimeout=15000", servers.get(0));
        assertEquals("failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000", servers.get(1));

        args[1] = "failover:(uri1,uri2)";
        args[2] = "failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000";
        servers = BalboaJms.parseServers(args);
        assertEquals(2, servers.size());
        assertEquals("failover:(uri1,uri2)?soTimeout=15000&soWriteTimeout=15000", servers.get(0));
        assertEquals("failover:(uri3,uri4)?soTimeout=15000&soWriteTimeout=15000", servers.get(1));

    }
}