package com.socrata.balboa.admin.tools;

import com.socrata.balboa.metrics.Timeslice;
import com.socrata.balboa.metrics.config.Configuration;
import com.socrata.balboa.metrics.data.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * List all the entity keys
 */
public class Lister {

    public void list(List<String> filters) throws IOException {
        {
            Period mostGranular = Period.mostGranular(Configuration.get().getSupportedPeriods());

            Date epoch = new Date(0);
            Date cutoff = DateRange.create(mostGranular, new Date()).start;

            DataStore ds = DataStoreFactory.get();
            Iterator<String> entities;
            if (filters.size() > 0)
            {
                List<Iterator<String>> iters = new ArrayList<Iterator<String>>(filters.size());
                for (String filter : filters)
                {
                    iters.add(ds.entities(filter));
                }
                entities = new CompoundIterator<String>(iters.toArray(new Iterator[] {}));
            }
            else
            {
                entities = ds.entities();
            }
            while (entities.hasNext())
            {
                System.out.println(entities.next());
            }
        }
    }
}
