package com.socrata.balboa.metrics.measurements.combining;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Summation implements Combinator<Number> {
    private BigDecimal sumBigDecimalWithOther(BigDecimal first, Number second) {
        if (!(second instanceof BigDecimal)) {
            second = new BigDecimal(second.toString());
        }

        return ((BigDecimal) first).add((BigDecimal) second);
    }

    Number sum(Number first, Number second) {
        // Find the best box in which both numbers fit.

        // BigDecimals should go first, since they're the highest priority.
        if (first instanceof BigDecimal) {
            return sumBigDecimalWithOther((BigDecimal) first, second);
        } else if (second instanceof BigDecimal) {
            return sumBigDecimalWithOther((BigDecimal) second, first);
        }

        // Next we have to try big decimals.
        if (first instanceof BigInteger && second instanceof BigInteger) {
            return ((BigInteger) first).add((BigInteger) second);
        } else if (first instanceof BigInteger) {
            return sumBigDecimalWithOther(new BigDecimal((BigInteger) first), second);
        } else if (second instanceof BigInteger) {
            return sumBigDecimalWithOther(new BigDecimal((BigInteger) second), first);
        }

        // Next we try doubles.
        if (first instanceof Double || first instanceof Float || second instanceof Double || second instanceof Float) {
            // Check for overflow first to see if we actually need to use a big
            // decimal for this.
            if (first.doubleValue() <= (Double.MAX_VALUE - second.doubleValue())) {
                return first.doubleValue() + second.doubleValue();
            } else {
                return sum(new BigDecimal(first.doubleValue()), new BigDecimal(second.doubleValue()));
            }
        }

        // If we've made it here, we know that we're looking a long or an integer. Add those badboys.
        if (first instanceof Long || second instanceof Long) {
            // Check for overflow first to see if we actually need to use a big
            // decimal for this.
            if (first.longValue() <= (Long.MAX_VALUE - second.longValue())) {
                return first.longValue() + second.longValue();
            } else {
                return sum(new BigDecimal(first.longValue()), new BigDecimal(second.longValue()));
            }
        } else {
            // Check for overflow first to see if we actually need to use a big
            // decimal for this.
            if (first.intValue() <= (Integer.MAX_VALUE - second.intValue())) {
                return first.intValue() + second.intValue();
            } else {
                // Since int + int should never be an overflow of a long, we
                // can feel confident not overflow checking this.
                return first.longValue() + second.longValue();
            }
        }
    }

    @Override
    public Number combine(Number first, Number second) {
        if (first == null) {
            first = 0;
        }

        if (second == null) {
            second = 0;
        }

        return sum(first, second);
    }
}
