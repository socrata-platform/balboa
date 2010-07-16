package com.socrata.balboa.metrics.measurements.serialization;

import com.google.protobuf.ByteString;
import com.socrata.balboa.metrics.measurements.serialization.impl.NumbersProtos;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ProtocolBuffersSerializer implements Serializer
{
    @Override
    public byte[] serialize(Object value) throws IOException
    {
        if (value == null)
        {
            throw new IllegalArgumentException("Serializing null values is unsupported in protocol buffers.");
        }

        if (value instanceof Double || value instanceof Float)
        {
            NumbersProtos.PBNumber doublenom = NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setDoubleValue(((Number)value).doubleValue()).
                    setType(NumbersProtos.PBNumber.Type.DOUBLE).
                    build();

            return doublenom.toByteArray();
        }
        else if (value instanceof Integer || value instanceof Short)
        {
            NumbersProtos.PBNumber intnom = NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setIntValue(((Number)value).intValue()).
                    setType(NumbersProtos.PBNumber.Type.INT).
                    build();

            return intnom.toByteArray();
        }
        else if (value instanceof Long)
        {
            NumbersProtos.PBNumber longnom = NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setLongValue(((Number)value).longValue()).
                    setType(NumbersProtos.PBNumber.Type.LONG).
                    build();

            return longnom.toByteArray();
        }
        else if (value instanceof BigDecimal)
        {
            BigDecimal v = (BigDecimal)value;
            BigInteger unscaled = v.movePointRight(v.scale()).toBigInteger();

            NumbersProtos.PBBigInteger bigpapa = NumbersProtos.
                    PBBigInteger.
                    newBuilder().
                    setValue(ByteString.copyFrom(unscaled.toByteArray())).
                    build();

            NumbersProtos.PBBigDecimal bigmomma = NumbersProtos.
                    PBBigDecimal.
                    newBuilder().
                    setScale(v.scale()).
                    setValue(bigpapa).
                    build();

            NumbersProtos.PBNumber bigdaddy = NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setType(NumbersProtos.PBNumber.Type.BIG_DECIMAL).
                    setBigDecimalValue(bigmomma).
                    build();

            return bigdaddy.toByteArray();
        }
        else
        {
            throw new IllegalArgumentException("Unsupported number serialization type '" + value.getClass().getSimpleName() + "'.");
        }
    }

    @Override
    public Object deserialize(byte[] serialized) throws IOException
    {
        NumbersProtos.PBNumber nomnom = NumbersProtos.PBNumber.parseFrom(serialized);

        switch (nomnom.getType())
        {
            case INT:
                return nomnom.getIntValue();
            case DOUBLE:
                return nomnom.getDoubleValue();
            case LONG:
                return nomnom.getLongValue();
            case BIG_DECIMAL:
                BigInteger bigmama = new BigInteger(nomnom.
                        getBigDecimalValue().
                        getValue(). // PBBigInteger
                        getValue(). // The actual BigInteger byte[]
                        toByteArray()
                );
                return new BigDecimal(bigmama, nomnom.getBigDecimalValue().getScale());
            default:
                throw new IOException("Unsupported or unknown serialized type '" + nomnom.getType() + "'.");
        }
    }
}
