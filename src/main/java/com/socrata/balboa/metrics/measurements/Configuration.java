package com.socrata.balboa.metrics.measurements;

import com.socrata.balboa.metrics.measurements.combining.Combinator;
import com.socrata.balboa.metrics.measurements.preprocessing.JsonPreprocessor;
import com.socrata.balboa.metrics.measurements.preprocessing.Preprocessor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Configuration
{
    private static Log log = LogFactory.getLog(Configuration.class);

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

    public static Configuration load(InputStream input) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();

        List<Object> configurations = (List<Object>)mapper.readValue(input, Object.class);

        Configuration config = new Configuration();

        for (Object object : configurations)
        {
            Map<String, Object> m = (Map<String, Object>)object;
            String combinatorName = (String)m.get("combinator");
            try
            {
                Combinator com = (Combinator)Class.forName("com.socrata.balboa.metrics.measurements.combining." + combinatorName).
                        getConstructor().
                        newInstance();
                
                config.add((String)m.get("field"), new JsonPreprocessor(), com);
            }
            catch (Exception e)
            {
                log.error("Unable to create a combinator class or unable to add it to the configuration. Ignoring.", e);
            }
        }

        return config;
    }
}
