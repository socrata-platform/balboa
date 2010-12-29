package com.socrata.balboa.server.rest;

import com.socrata.balboa.metrics.data.DataStore;
import com.socrata.balboa.metrics.data.DataStoreFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

@Path("/entities")
public class EntitiesRest
{
    static class FilteredIterator implements Iterator<String>
    {
        Pattern filter;
        Iterator<String> other;
        String next;
        int limit;
        int count = 0;

        FilteredIterator(Pattern filter, Iterator<String> other, int limit)
        {
            this.filter = filter;
            this.other = other;
            this.limit = limit;
        }

        @Override
        public boolean hasNext()
        {
            if (next != null)
            {
                return true;
            }

            if (limit >= 0 && count >= limit)
            {
                return false;
            }

            if (filter != null)
            {
                while (other.hasNext() && !filter.matcher(next = other.next()).matches()) {}
            }

            if (other.hasNext() && next == null)
            {
                next = other.next();
                count += 1;
                return true;
            }
            else if (next != null)
            {
                count += 1;
                return true;
            }
            else
            {
                return false;
            }
        }

        @Override
        public String next()
        {
            if (hasNext())
            {
                String result = next;
                next = null;
                return result;
            }
            else
            {
                throw new NoSuchElementException("There are no more summaries.");
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
    @GET
    @Produces("application/json")
    public Response index(
            @QueryParam("filter") String filter,
            @DefaultValue("-1") @QueryParam("limit") int limit
    ) throws IOException
    {
        DataStore ds = DataStoreFactory.get();
        Pattern pattern = null;

        if (filter != null)
        {
             pattern = Pattern.compile(filter);
        }

        return render(new FilteredIterator(pattern, ds.entities(), limit));
    }

    public Response render(Iterator<? extends String> entityIds) throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);

        return Response.ok(mapper.writeValueAsString(entityIds), "application/json").build();
    }
}
