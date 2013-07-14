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

import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.service.thrift.*;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.nio.file.*;

/**
 * NadeefServiceHandler handles request for NADEEF service.
 */
// TODO: speedup the compiling stage by using object caching.
public class NadeefServiceHandler implements TNadeefService.Iface {
    private static Tracer tracer = Tracer.getTracer(NadeefServiceHandler.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public String generate(TRule rule) throws TException {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean verify(TRule rule) {
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

                    if (CommonTools.compileFile(outputPath.toFile())) {
                        return true;
                    };
                    break;
                case FD:
                    break;
                case CFD:
                    break;
            }
        } catch (Exception ex) {
            tracer.err("Exception happens in verify.", ex);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String detect(TRule rule, String tableName) throws TNadeefRemoteException {
        if (!verify(rule)) {
            TNadeefRemoteException ex = new TNadeefRemoteException();
            ex.setType(TNadeefExceptionType.COMPILE_ERROR);
            throw ex;
        }

        try {
            String name = rule.getName();
            Class udfClass = CommonTools.loadClass(name);
            if (!Rule.class.isAssignableFrom(udfClass)) {
                throw new InvalidRuleException("The specified class is not a Rule class.");
            }

            Rule ruleInstance = (Rule) udfClass.newInstance();
            ruleInstance.initialize(rule.getName(), Lists.newArrayList(tableName));
            DBConfig config = new DBConfig(NadeefConfiguration.getDbConfig());

            NadeefJobScheduler scheduler = NadeefJobScheduler.getInstance();
            String key = scheduler.submitDetectJob(new CleanPlan(config, ruleInstance));
            return key;
        } catch (InvalidRuleException ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.INVALID_RULE);
            re.setMessage(ex.getMessage());
            throw re;
        } catch (Exception ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.UNKNOWN);
            re.setMessage(ex.getMessage());
            throw re;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String repair(TRule rule, String tableName) throws TNadeefRemoteException {
        if (!verify(rule)) {
            TNadeefRemoteException ex = new TNadeefRemoteException();
            ex.setType(TNadeefExceptionType.COMPILE_ERROR);
            throw ex;
        }

        try {
            String name = rule.getName();
            Class udfClass = CommonTools.loadClass(name);
            if (!Rule.class.isAssignableFrom(udfClass)) {
                throw new InvalidRuleException("The specified class is not a Rule class.");
            }

            Rule ruleInstance = (Rule) udfClass.newInstance();
            ruleInstance.initialize(rule.getName(), Lists.newArrayList(tableName));
            DBConfig config = new DBConfig(NadeefConfiguration.getDbConfig());

            NadeefJobScheduler scheduler = NadeefJobScheduler.getInstance();
            String key = scheduler.submitRepairJob(new CleanPlan(config, ruleInstance));
            return key;
        } catch (InvalidRuleException ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.INVALID_RULE);
            re.setMessage(ex.getMessage());
            throw re;
        } catch (Exception ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.UNKNOWN);
            re.setMessage(ex.getMessage());
            throw re;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TJobStatus getJobStatus(String key) throws TException {
        NadeefJobScheduler jobScheduler = NadeefJobScheduler.getInstance();
        return jobScheduler.getJobStatus(key);
    }
}
