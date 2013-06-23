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

package qa.qcri.nadeef.test.core;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.pipeline.Flow;
import qa.qcri.nadeef.core.pipeline.Iterator;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.pipeline.ViolationDetector;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.test.udf.MyTestRule1;
import qa.qcri.nadeef.tools.Tracer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IteratorStreamTest {
    @Before
    public void before() {
        Bootstrap.start();
        Tracer.setVerbose(true);
    }

    @Test
    public void test() {
        Flow iflow = new Flow("iterator");
        Flow dflow = new Flow("detector");

        Rule rule = new MyTestRule1();
        rule.initialize("testRule", Lists.newArrayList("test"));
        NodeCacheManager cacheManager = NodeCacheManager.getInstance();
        Schema.Builder builder = new Schema.Builder();
        Schema schema = builder.table("test").column("A").column("B").build();
        List values = Arrays.asList("a", "b");

        List<Tuple> tuples = Lists.newArrayList();
        for (int i = 1; i < 100; i ++) {
            tuples.add(new Tuple(i, schema, values));
        }
        MemoryTable table = MemoryTable.of(tuples);
        String key = cacheManager.put(Lists.newArrayList(table));
        String ruleKey = cacheManager.put(rule);
        iflow.setInputKey(key);
        iflow.addNode(new Iterator<Tuple>(rule));

        dflow.setInputKey(ruleKey);
        iflow.start();
        dflow.start();
        iflow.waitUntilFinish();
        dflow.waitUntilFinish();
    }
}
