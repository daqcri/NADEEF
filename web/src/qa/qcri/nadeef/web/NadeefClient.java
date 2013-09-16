/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.web;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import qa.qcri.nadeef.service.thrift.TJobStatus;
import qa.qcri.nadeef.service.thrift.TNadeefService;
import qa.qcri.nadeef.service.thrift.TRule;

import java.util.List;

/**
 * Nadeef Thrift client.
 */
public final class NadeefClient {
    private static NadeefClient instance;

    private TNadeefService.Client client;
    private TTransport transport;

    /**
     * Gets the instance of client.
     * @return instance.
     */
    public synchronized static NadeefClient getInstance(
        String url,
        int port
    ) throws TTransportException {
        if (instance == null) {
            instance = new NadeefClient(url, port);
        }
        return instance;
    }

    private NadeefClient(String url, int port) throws TTransportException {
        transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        client = new TNadeefService.Client(protocol);
    }

    /**
     * Shutdown the client.
     */
    public void shutdown() {
        if (transport != null) {
            transport.close();
        }
    }

    public String generate(
        String type,
        String name,
        String code,
        String table1
    ) throws TException {
        TRule rule = new TRule(name, type, code);
        JSONObject result = new JSONObject();
        result.put("data", client.generate(rule, table1));
        return result.toJSONString();
    }

    public String verify(String type, String name, String code) throws TException {
        JSONObject result = new JSONObject();
        result.put("data", client.verify(new TRule(name, type, code)));
        return result.toJSONString();
    }

    public String detect(
        String type,
        String name,
        String code,
        String table1,
        String table2
    ) throws TException {
        TRule rule = new TRule(name, type, code);
        JSONObject result = new JSONObject();
        result.put("data", client.detect(rule, table1, table2));
        return result.toJSONString();
    }

    public String repair(
        String type,
        String name,
        String code,
        String table1,
        String table2
    ) throws TException {
        TRule rule = new TRule(name, type, code);
        JSONObject result = new JSONObject();
        result.put("data", client.repair(rule, table1, table2));
        return result.toJSONString();
    }

    public String getJobStatus() throws TException {
        JSONObject result = new JSONObject();
        List<TJobStatus> statusList = client.getAllJobStatus();
        JSONArray jsonArray = new JSONArray();

        for (TJobStatus status : statusList) {
            JSONObject obj = new JSONObject();
            obj.put("status", status.getStatus());
            obj.put("overallProgress", status.getOverallProgress());
            obj.put("key", status.getKey());
            obj.put("progress", status.getProgress());
            obj.put("name", status.getNames());
            jsonArray.add(obj);
        }
        result.put("data", jsonArray);
        return result.toJSONString();
    }
}
