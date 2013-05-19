/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.TuplePair;

import java.io.File;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

class Producer implements Runnable {
    private BlockingQueue<List> queue;
    private int size;
    private final int BufferSize = 1024;

    public Producer(BlockingQueue<List> queue, int size) {
        this.queue = queue;
        this.size = size;
    }

    @Override
    public void run() {
        Column column1 = new Column("test.a");
        Column column2 = new Column("test.b");
        Column column3 = new Column("test.c");
        Column column4 = new Column("test.d");
        Column column5 = new Column("test.e");
        Object[] values1 = { "a", "b", "c", "d", "e" };
        Object[] values2 = { "a", "b", "c", "d", "e" };

        Schema schema =
            new Schema(
                "test",
                Lists.newArrayList(column1, column2, column3, column4, column5)
            );

        Tuple left = new Tuple(1, schema, values1);
        Tuple right = new Tuple(1, schema, values2);
        try {
            List<TuplePair> buffer = Lists.newArrayList();
            for (int i = 0; i < size; i ++) {
                for (int j = i + 1; j < size; j ++) {
                    TuplePair pair = new TuplePair(left, right);
                    buffer.add(pair);
                    if (buffer.size() == BufferSize) {
                        while (!queue.offer(buffer, 1024, TimeUnit.MICROSECONDS));
                        buffer = Lists.newArrayList();
                    }
                }
            }
            queue.put(Lists.newArrayList());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}


class Consumer implements Runnable {
    private BlockingQueue<List> queue;

    public Consumer(BlockingQueue<List> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                List<TuplePair> pairs;
                while ((pairs = queue.poll(1024, TimeUnit.MICROSECONDS)) == null);

                if (pairs.size() == 0) {
                    break;
                }

                for (TuplePair pair : pairs) {
                    dummyDetect(pair);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void dummyDetect(TuplePair pair) {
        Tuple left = pair.getLeft();
        Tuple right = pair.getRight();
        if (!left.hasSameValue(right)) {
            System.out.println("Impossible");
        }
    }
}

class RecorderTask extends TimerTask {
    private BlockingQueue<List> queue;
    public List<Integer> capacityRecord;


    public RecorderTask(BlockingQueue<List> queue) {
        capacityRecord = Lists.newArrayList();
        this.queue = queue;
    }

    /**
     * The action to be performed by this timer task.
     */
    @Override
    public void run() {
        capacityRecord.add(queue.size());
    }
}
/**
 * BlockingQueue Test.
 */
public class BlockingQueueTest {
    private BlockingQueue<List> blockingQueue;
    private Timer timer;
    private RecorderTask recorderTask;

    @Before
    public void setup() {
        blockingQueue = new LinkedBlockingQueue<List>(8192);
        timer = new Timer();
        recorderTask = new RecorderTask(blockingQueue);
    }

    @Test
    public void perfTest() {
         long testTupleSize = 102400;

        try {
            Producer producer = new Producer(blockingQueue, (int)testTupleSize);
            Consumer consumer = new Consumer(blockingQueue);

            Thread producerThread = new Thread(producer);
            producerThread.setName("producer");
            Thread consumerThread = new Thread(consumer);
            consumerThread.setName("consumer");

            // start the recorder.
            new Thread() {
                public void run() {
                    // call every 500 ms
                    timer.schedule(recorderTask, 0, 100);
                }
            }.start();

            Stopwatch stopwatch = new Stopwatch().start();

            consumerThread.start();
            producerThread.start();

            producerThread.join();
            consumerThread.join();

            double totalTuple = testTupleSize * testTupleSize;
            double elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            System.out.format(
                "TotalTime: %d ms TotalTuple: %d Throughput: %.2f tuple / ms\n",
                (long)elapsedTime,
                (long)totalTuple,
                totalTuple / elapsedTime
            );

            // stop the timer.
            timer.cancel();
            stopwatch.stop();

            // write the queue capacity graph
            String output = Joiner.on("\r\n").join(recorderTask.capacityRecord);
            Files.write(output.getBytes(), new File("perf.csv"));

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
