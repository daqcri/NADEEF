/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.tools.Tracer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Streaming output (Bounded Queued Buffer).
 * TODO: make it a generic data structure.
 */
public class IteratorStream<E> {
    private static final long TIMEOUT = 1024;
    private static final int BUFFER_BOUNDARY = 1024;

    private static Tracer tracer = Tracer.getTracer(IteratorStream.class);

    private static LinkedBlockingQueue<List> queue = new LinkedBlockingQueue<List>();
    private List<E> buffer;

    /**
     * Constructor.
     */
    public IteratorStream() {
        this.buffer = new ArrayList<E>(BUFFER_BOUNDARY);
    }

    /**
     * Gets a buffer of objects from the queue.
     * @return a list of objects from the queue.
     */
    @SuppressWarnings("unchecked")
    public List<E> poll() {
        List<E> item;
        while ((item = queue.poll()) == null);
        return item;
    }

    /**
     * Marks the end of the iteration output.
     */
    public static void markEnd() {
        try {
            List endList = Lists.newArrayList();
            while (!queue.offer(endList, TIMEOUT, TimeUnit.MILLISECONDS));
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
        if (buffer.size() == BUFFER_BOUNDARY) {
            try {
                while (!queue.offer(buffer, TIMEOUT, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            buffer = Lists.newArrayList();
        }

        buffer.add((E)item);
    }

    /**
     * Flush the remaining buffer.
     */
    public void flush() {
        try {
            if (buffer.size() != 0)
                while (!queue.offer(buffer, TIMEOUT, TimeUnit.MILLISECONDS));
            buffer = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Clear the buffer queue.
     */
    public static void clear() {
        queue.clear();
    }
}
