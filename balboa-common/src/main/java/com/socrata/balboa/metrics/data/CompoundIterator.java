package com.socrata.balboa.metrics.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CompoundIterator<T> implements Iterator<T>
{
    List<Iterator<T>> iters = new ArrayList<Iterator<T>>();

    public CompoundIterator()
    {
    }

    @SafeVarargs
    public CompoundIterator(Iterator<T>... everything)
    {
        for (Iterator<T> iter : everything)
        {
            add(iter);
        }
    }

    public void add(Iterator<T> robo)
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
    public T next()
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
