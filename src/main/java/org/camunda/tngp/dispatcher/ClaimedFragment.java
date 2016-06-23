package org.camunda.tngp.dispatcher;

import static org.camunda.tngp.dispatcher.impl.log.DataFrameDescriptor.*;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Represents a claimed fragment in the buffer.
 *
 * Reusable but not threadsafe.
 *
 */
public class ClaimedFragment
{

    protected final UnsafeBuffer buffer;

    public ClaimedFragment()
    {
        buffer = new UnsafeBuffer(0, 0);
    }

    public void wrap(UnsafeBuffer underlyingbuffer, int fragmentOffset, int fragmentLength)
    {
        buffer.wrap(underlyingbuffer, fragmentOffset, fragmentLength);
    }

    public int getOffset()
    {
        return HEADER_LENGTH;
    }

    public int getLength()
    {
        return buffer.capacity() - HEADER_LENGTH;
    }

    public int getFragmentLength()
    {
        return buffer.capacity();
    }

    public MutableDirectBuffer getBuffer()
    {
        return buffer;
    }

    public void commit()
    {
        // commit the message by writing the positive length
        buffer.putIntOrdered(0, buffer.capacity() - HEADER_LENGTH);
        reset(buffer);
    }

    public void abort()
    {
        // abort the message by setting type to padding and writing the positive length
        buffer.putInt(typeOffset(0), TYPE_PADDING);
        buffer.putIntOrdered(0, buffer.capacity() - HEADER_LENGTH);
        reset(buffer);
    }

    private static void reset(UnsafeBuffer fragmentWrapper)
    {
        fragmentWrapper.wrap(0, 0);
    }

    public boolean isOpen()
    {
        return getFragmentLength() > 0;
    }

}
