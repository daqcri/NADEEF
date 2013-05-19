/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Iterator output.
 */
public class IteratorOutput<E> {
    private final long TIMEOUT = 1024;
    private final int BUFFER_BOUNDARY = 512;

    private LinkedBlockingQueue<List<E>> queue;
    private List<E> buffer;

    /**
     * Constructor.
     * @param queue Blocking queue.
     */
    public IteratorOutput(LinkedBlockingQueue<List<E>> queue) {
        this.queue = Preconditions.checkNotNull(queue);
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
        } else {
            buffer.add(item);
        }
    }

}
