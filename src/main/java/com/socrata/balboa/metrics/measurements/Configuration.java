package com.socrata.balboa.metrics.measurements;

import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.preprocessing.Preprocessor;

import java.util.ArrayList;
import java.util.List;

public class Configuration
{
    static class Measurement
    {
        String field;
        Preprocessor preprocessor;
        Combinator combinator;

        public Measurement(String field, Preprocessor preprocessor, Combinator combinator)
        {
            this.field = field;
            this.preprocessor = preprocessor;
            this.combinator = combinator;
        }

        public String getField()
        {
            return field;
        }

        public Preprocessor getPreprocessor()
        {
            return preprocessor;
        }

        public Combinator getCombinator()
        {
            return combinator;
        }
    }

    List<Measurement> measurements = new ArrayList<Measurement>();

    public void add(String field, Preprocessor pre, Combinator com)
    {
        measurements.add(new Measurement(field, pre, com));
    }

    public final List<Measurement> getMeasurements()
    {
        return measurements;
    }
}
