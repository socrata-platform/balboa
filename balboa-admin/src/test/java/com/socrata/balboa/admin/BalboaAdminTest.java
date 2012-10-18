package com.socrata.balboa.admin;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;

public class BalboaAdminTest {
    @Test
    public void testFindSomeEscapeCharacter() throws Exception {
        File test = File.createTempFile("balboa", "admin");
        FileOutputStream fos = new FileOutputStream(test);
        fos.write(0);
        fos.write(1);
        fos.write(2);
        fos.write(3);
        fos.write(5);
        fos.close();
        char c = BalboaAdmin.pickEscapeCharacter(test);
        System.out.println("Hey we got " + ((int)c) + " that means we rock!");
        assertEquals(4, c);
        test.delete();
    }
}
