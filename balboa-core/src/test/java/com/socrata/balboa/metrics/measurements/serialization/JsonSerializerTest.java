package com.socrata.balboa.metrics.measurements.serialization;

import org.junit.Test;

public class JsonSerializerTest
{
    @Test
    public void stfu() throws Exception
    {}
    
    /*@Test
    public void testInt() throws Exception
    {
        Serializer ser = new JsonSerializer();

        int ints[] = {25, -10, 0};

        for (int i : ints)
        {
            int hopefullyTheSame = (Integer)ser.deserialize(ser.serialize(i));
            Assert.assertEquals(i, hopefullyTheSame);
        }
    }

    @Test
    public void testFloat() throws Exception
    {
        Serializer ser = new JsonSerializer();

        float floats[] = {0.0f, 25f, -100f};
        double doubles[] = {0.0d, 25d, -100d};

        for (int i=0; i < floats.length; i++)
        {
            double hopefullyTheSame = (Double)ser.deserialize(ser.serialize(floats[i]));
            Assert.assertEquals(doubles[i], hopefullyTheSame, 0.001);
        }
    }

    @Test
    public void testDouble() throws Exception
    {
        Serializer ser = new JsonSerializer();

        double doubles[] = {0.0d, 25d, -100d, Float.MAX_VALUE, Float.MIN_VALUE, Double.MAX_VALUE, Double.MIN_VALUE};

        for (double d : doubles)
        {
            double hopefullyTheSame = (Double)ser.deserialize(ser.serialize(d));
            Assert.assertEquals(d, hopefullyTheSame, 0.001);
        }
    }

    @Test
    public void testLong() throws Exception
    {
        Serializer ser = new JsonSerializer();

        long longs[] = {Integer.MIN_VALUE, Integer.MAX_VALUE};

        for (long l : longs)
        {
            long hopefullyTheSame = (Long)ser.deserialize(ser.serialize(l));
            Assert.assertEquals(l, hopefullyTheSame);
        }
    }*/
}
