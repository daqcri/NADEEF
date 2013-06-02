/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.core.datamodel.TuplePair;
import qa.qcri.nadeef.core.pipeline.Operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Custom iterator.
 */
public class MyIterator1 extends Operator<Collection<Table>, Collection<TuplePair>> {
    /**
     * Execute the operator.
     *
     * @param tables input tuples.
     * @return output object.
     */
    @Override
    public Collection<TuplePair> execute(Collection<Table> tables)
        throws Exception {
		System.out.println("calling my iterator");
        Collection<TuplePair> result = new ArrayList();
        List<Table> collectionList = Lists.newArrayList(tables);
        for (Table tuples : collectionList) {
            for (int i = 0; i < tuples.size(); i ++) {
                for (int j = i + 1; j < tuples.size(); j ++) {
                    TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                    result.add(pair);
                }
            }
        }
        return result;
    }
}
