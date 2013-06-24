/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.datamodel;

import qa.qcri.nadeef.tools.Tracer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Streaming output (Bounded Queued Buffer).
 */
// TODO: make it a generic data structure.
public class IteratorStream<E> {
    private static final long TIMEOUT = 1024;
    private static final int BUFFER_BOUNDARY = 10240;
    private static final int MAX_QUEUE_BOUNDARY = 1024;
    private static Tracer tracer = Tracer.getTracer(IteratorStream.class);
    private static LinkedBlockingQueue<Object[]> queue =
        new LinkedBlockingQueue<>(MAX_QUEUE_BOUNDARY);
    private Object[] buffer;

    private int size;

    /**
     * Constructor.
     */
    public IteratorStream() {
        this.buffer = new Object[BUFFER_BOUNDARY];
        this.size = 0;
    }

    /**
     * Gets a buffer of objects from the queue.
     * @return a list of objects from the queue.
     */
    @SuppressWarnings("unchecked")
    public Object[] poll() {
        Object[] item = null;
        try {
            while ((item = queue.poll(TIMEOUT, TimeUnit.MILLISECONDS)) == null);
        } catch (InterruptedException ex) {
            tracer.err("Exception during polling the queue.", ex);
        }
        return item;
    }

    /**
     * Marks the end of the iteration output.
     */
    public static void markEnd() {
        try {
            Object[] end = new Object[0];
            while (!queue.offer(end, TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (InterruptedException ex) {
            tracer.err("Exception during marking the end of the queue.", ex);
        }
    }

    /**
     * Puts the item in the buffer.
     * @param item item.
     */
    @SuppressWarnings("unchecked")
    public void put(Object item) {
        if (size == BUFFER_BOUNDARY) {
            try {
                while (!queue.offer(buffer, TIMEOUT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                tracer.err("put interrupted", e);
            }
            buffer = new Object[BUFFER_BOUNDARY];
            size = 0;
        }

        buffer[size ++] = item;
    }

    /**
     * Flush the remaining buffer.
     */
    public void flush() {
        try {
            if (size != 0) {
                while (!queue.offer(buffer, TIMEOUT, TimeUnit.MILLISECONDS));
            }
            buffer = null;
            size = 0;
        } catch (InterruptedException e) {
            tracer.err("flush interrupted", e);
        }
    }

    /**
     * Clear the buffer queue.
     */
    public static void clear() {
        queue.clear();
    }
}
