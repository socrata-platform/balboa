package com.socrata.balboa.metrics.data;

import com.socrata.balboa.metrics.Summary;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CompoundIterator implements Iterator<Summary>
{
    List<Iterator<Summary>> iters = new ArrayList<Iterator<Summary>>();

    public void add(Iterator<Summary> robo)
    {
        iters.add(robo);
    }

    @Override
    public boolean hasNext()
    {
        if (iters.size() > 0 && iters.get(0).hasNext())
        {
            return true;
        }
        else if (iters.size() > 0 && !iters.get(0).hasNext())
        {
            iters.remove(0);
            return hasNext();
        }
        else
        {
            return false;
        }
    }

    @Override
    public Summary next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException("There are no more summaries.");
        }
        else
        {
            return iters.get(0).next();
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException("Not supported.");
    }
}
