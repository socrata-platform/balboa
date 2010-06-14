package com.socrata.balboa.metrics.measurements.combining;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Sum implements Combinator<Number>
{
    private BigDecimal sumBigDecimalWithOther(BigDecimal first, Number second)
    {
        if (!(second instanceof BigDecimal))
        {
            second = new BigDecimal(second.toString());
        }

        return ((BigDecimal)first).add((BigDecimal)second);
    }

    Number sum(Number first, Number second)
    {
        // Find the best box in which both numbers fit.

        // BigDecimals should go first, since they're the highest priority.
        if (first instanceof BigDecimal)
        {
            return sumBigDecimalWithOther((BigDecimal)first, second);
        }
        else if (second instanceof BigDecimal)
        {
            return sumBigDecimalWithOther((BigDecimal)second, first);
        }

        // Next we have to try big decimals.
        if (first instanceof BigInteger && second instanceof BigInteger)
        {
            return ((BigInteger)first).add((BigInteger)second);
        }
        else if (first instanceof BigInteger)
        {
            return sumBigDecimalWithOther(new BigDecimal((BigInteger)first), second);
        }
        else if (second instanceof BigInteger)
        {
            return sumBigDecimalWithOther(new BigDecimal((BigInteger)second), first);
        }

        // Next we try doubles.
        if (first instanceof Double || first instanceof Float || second instanceof Double || second instanceof Float)
        {
            return first.doubleValue() + second.doubleValue();
        }

        // If we've made it here, we know that we're looking a long or an integer. Add those badboys.
        if (first instanceof Long || second instanceof Long)
        {
            return first.longValue() + second.longValue();
        }
        else
        {
            return first.intValue() + second.intValue();
        }
    }

    @Override
    public Number combine(Number first, Number second)
    {
        if (first == null)
        {
            first = 0;
        }
        
        if (second == null)
        {
            second = 0;
        }

        return sum(first, second);
    }
}
