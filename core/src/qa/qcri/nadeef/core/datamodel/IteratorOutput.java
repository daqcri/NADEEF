/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.tools.Tracer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Iterator output.
 */
public class IteratorOutput<E> {
    private final long TIMEOUT = 1024;
    private final int BUFFER_BOUNDARY = 512;

    private static Tracer tracer = Tracer.getTracer(IteratorOutput.class);

    private LinkedBlockingQueue<List<E>> queue;
    private List<E> buffer;

    /**
     * Constructor.
     */
    public IteratorOutput() {
        this.queue = new LinkedBlockingQueue<List<E>>();
        this.buffer = Lists.newArrayList();
    }

    /**
     * Gets a buffer of objects from the queue.
     * @return a list of objects from the queue.
     */
    public List<E> poll() {
        List<E> item;

        while ((item = queue.poll()) == null);
        return item;
    }

    /**
     * Marks the end of the iteration output.
     */
    public void markEnd() {
        try {
            List<E> endList = Lists.newArrayList();
            while (!queue.offer(buffer, TIMEOUT, TimeUnit.MILLISECONDS));
            while (!queue.offer(endList, TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException ex) {
            tracer.err("Exception during marking the end of the queue.", ex);
        }
    }

    /**
     * Puts the item in the buffer.
     * @param item item.
     */
    public synchronized void put(E item) {
        if (buffer.size() == BUFFER_BOUNDARY) {
            try {
                while (!queue.offer(buffer, TIMEOUT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            buffer = Lists.newArrayList();
        }

        buffer.add(item);
    }
}
