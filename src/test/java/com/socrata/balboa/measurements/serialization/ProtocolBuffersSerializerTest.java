package com.socrata.balboa.measurements.serialization;

import com.socrata.balboa.metrics.measurements.serialization.ProtocolBuffersSerializer;
import com.socrata.balboa.metrics.measurements.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class ProtocolBuffersSerializerTest
{
    @Test
    public void testIntOverflow() throws Exception
    {
        Serializer ser = new ProtocolBuffersSerializer();
        int hopefullyTheSame = (Integer)ser.deserialize(ser.serialize(128));
        Assert.assertEquals(128, hopefullyTheSame);
    }

    @Test
    public void testInt() throws Exception
    {
        Serializer ser = new ProtocolBuffersSerializer();

        int ints[] = {25, -10, 0, Integer.MIN_VALUE, Integer.MAX_VALUE};

        for (int i : ints)
        {
            int hopefullyTheSame = (Integer)ser.deserialize(ser.serialize(i));
            Assert.assertEquals(i, hopefullyTheSame);
        }
    }

    @Test
    public void testFloat() throws Exception
    {
        Serializer ser = new ProtocolBuffersSerializer();

        float floats[] = {0.0f, 25f, -100f, Float.MAX_VALUE, Float.MIN_VALUE};
        double doubles[] = {0.0d, 25d, -100d, Float.MAX_VALUE, Float.MIN_VALUE};

        for (int i=0; i < floats.length; i++)
        {
            double hopefullyTheSame = (Double)ser.deserialize(ser.serialize(floats[i]));
            Assert.assertEquals(doubles[i], hopefullyTheSame, 0.001);
        }
    }

    @Test
    public void testDouble() throws Exception
    {
        Serializer ser = new ProtocolBuffersSerializer();

        double doubles[] = {0.0d, 25d, -100d, Double.MAX_VALUE, Double.MIN_VALUE};

        for (double d : doubles)
        {
            double hopefullyTheSame = (Double)ser.deserialize(ser.serialize(d));
            Assert.assertEquals(d, hopefullyTheSame, 0.001);
        }
    }

    @Test
    public void testLong() throws Exception
    {
        Serializer ser = new ProtocolBuffersSerializer();

        long longs[] = {0l, 25l, -100l, Long.MAX_VALUE, Long.MIN_VALUE};

        for (long l : longs)
        {
            long hopefullyTheSame = (Long)ser.deserialize(ser.serialize(l));
            Assert.assertEquals(l, hopefullyTheSame);
        }
    }

    @Test
    public void testBigDecimal() throws Exception
    {
        Serializer ser = new ProtocolBuffersSerializer();

        BigDecimal bds[] = {
                new BigDecimal(0),
                new BigDecimal("-100"),
                new BigDecimal("100.00"),
                new BigDecimal("-100.001"),
                new BigDecimal(new Long(Long.MAX_VALUE).toString() + "12341"),
                new BigDecimal("-" + new Long(Long.MAX_VALUE).toString() + "1214.1231241")
        };

        for (BigDecimal b : bds)
        {
            BigDecimal hopefullyTheSame = (BigDecimal)ser.deserialize(ser.serialize(b));
            Assert.assertEquals(b, hopefullyTheSame);
        }
    }
}
