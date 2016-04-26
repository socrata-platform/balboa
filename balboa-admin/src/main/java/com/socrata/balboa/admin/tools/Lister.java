package com.socrata.balboa.admin.tools;

import com.socrata.balboa.metrics.data.CompoundIterator;
import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * List all the entity keys
 */
public class Lister {

    public void list(List<String> filters) throws IOException {
        {
            DataStore ds = DataStoreFactory.get();
            Iterator<String> entities;
            if (filters.size() > 0)
            {
                List<Iterator<String>> iters = new ArrayList<>(filters.size());
                for (String filter : filters)
                {
                    iters.add(ds.entities(filter));
                }
                entities = new CompoundIterator<>(iters);
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
