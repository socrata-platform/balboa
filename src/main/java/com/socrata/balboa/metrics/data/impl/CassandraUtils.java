package com.socrata.balboa.metrics.data.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class CassandraUtils
{
    public static byte[] packLong(long timestamp) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(timestamp);
        dos.flush();
        return bos.toByteArray();
    }

    public static Long unpackLong(byte[] data)
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);
        try
        {
            return dis.readLong();
        }
        catch (IOException e)
        {
            return null;
        }
    }
}
