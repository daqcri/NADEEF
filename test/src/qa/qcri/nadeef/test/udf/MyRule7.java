/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic.
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */
package qa.qcri.nadeef.test.udf;

import org.json.simple.parser.ParseException;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.*;

public class MyRule7 extends SingleTupleRule {
    private List<Predicate> predicates;

    public MyRule7() throws ParseException{
        predicates = new ArrayList<>();
    }

    @Override
    public Collection<Violation> detect(Tuple tuple) {
        String tableName = tableNames.get(0);
        predicates.add(Predicate.valueOf("t1.B!=b1", tableName));
        boolean isValid = true;
        List<Violation> result = new ArrayList<>();
        Set<Cell> infectedCells = new HashSet<>();
        for (Predicate predicate : predicates){
            if (!predicate.isValid(tuple)) {
                isValid = false;
                break;
            }

            Cell leftCell = tuple.getCell(predicate.getLeft());
            infectedCells.add(leftCell);
            if (!predicate.isRightConstant()){
                Cell rightCell = tuple.getCell(predicate.getRight());
                infectedCells.add(rightCell);
            }
        }
        // all the predicates are valid, then the DC is violated
        if (isValid){
            Violation violation = new Violation(ruleName);
            for (Cell cell : infectedCells){
                violation.addCell(cell);
            }
            //violation.addCell(cell);
            result.add(violation);
        }
        return result;
    }

    @Override
    public Collection<Fix> repair(Violation violation) {
        // TODO Auto-generated method stub
        return null;
    }
}