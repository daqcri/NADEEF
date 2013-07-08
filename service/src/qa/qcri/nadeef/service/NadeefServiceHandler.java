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

package qa.qcri.nadeef.service;

import org.apache.thrift.TException;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.service.thrift.TNadeefService;
import qa.qcri.nadeef.service.thrift.TRule;
import qa.qcri.nadeef.service.thrift.TRuleType;
import qa.qcri.nadeef.tools.CommonTools;

import java.nio.file.*;

public class NadeefServiceHandler implements TNadeefService.Iface {
    @Override
    public String generate(TRule rule) throws org.apache.thrift.TException {
        System.out.println("calling generate");
        System.out.println("code " + rule.getCode());
        System.out.println("name " + rule.getName());
        System.out.println("type " + rule.getType());
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return "";
    }

    /**
     * Verify the given rule object.
     * @param rule given {@link TRule} object.
     * @return 0 when the rule is correct, otherwise returns 1.
     */
    @Override
    public int verify(TRule rule) throws TException {
        TRuleType type = rule.getType();
        String code = rule.getCode();
        String name = rule.getName();

        try {
            switch (type) {
                case UDF:
                    Path outputPath =
                        FileSystems.getDefault().getPath(
                            NadeefConfiguration.getOutputPath().toString(),
                            name + ".java"
                        );

                    Files.write(
                        outputPath,
                        code.getBytes(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                    );

                    Class udfClass = CommonTools.loadClass(name);
                    if (!Rule.class.isAssignableFrom(udfClass)) {
                        throw new InvalidRuleException(
                            "The specified class is not a Rule class."
                        );
                    }
                    break;
                case FD:
                    break;
                case CFD:
                    break;
            }
        } catch (Exception ex) {
            throw new TException(ex.getMessage());
        }
        return 0;
    }
}
