package com.socrata.balboa.metrics.measurements.serialization;

import com.google.protobuf.ByteString;
import com.socrata.balboa.metrics.measurements.serialization.impl.NumbersProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ProtocolBuffersSerializer implements Serializer
{
    private static final Logger log = LoggerFactory.getLogger(ProtocolBuffersSerializer.class);

    public NumbersProtos.PBNumber proto(Number value)
    {
        if (value instanceof Double || value instanceof Float)
        {
            return NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setDoubleValue(((Number)value).doubleValue()).
                    setType(NumbersProtos.PBNumber.Type.DOUBLE).
                    build();
        }
        else if (value instanceof Integer || value instanceof Short)
        {
            return NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setIntValue(((Number)value).intValue()).
                    setType(NumbersProtos.PBNumber.Type.INT).
                    build();
        }
        else if (value instanceof Long)
        {
            return NumbersProtos.
                    PBNumber.
                    newBuilder().
                    setLongValue(((Number)value).longValue()).
                    setType(NumbersProtos.PBNumber.Type.LONG).
                    build();
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

            return bigdaddy;
        }
        else
        {
            throw new IllegalArgumentException("Unsupported number serialization type '" + value.getClass().getSimpleName() + "'.");
        }


    }

    @Override
    public byte[] serialize(Object value) throws IOException
    {
        if (value == null)
        {
            throw new IllegalArgumentException("Serializing null values is unsupported in protocol buffers.");
        }
        else if (!(value instanceof Number))
        {
            throw new IllegalArgumentException("Cannot serialize non-number types.");
        }

        log.trace("Preparing to serialize " + value + ":" + value.getClass().getSimpleName() + ".");

        return proto((Number)value).toByteArray();
    }

    @Override
    public Object deserialize(byte[] serialized) throws IOException
    {
        NumbersProtos.PBNumber nomnom = NumbersProtos.PBNumber.parseFrom(serialized);

        return java(nomnom);
    }

    public Number java(NumbersProtos.PBNumber nomnom) throws IOException
    {
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
