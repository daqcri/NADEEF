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

import com.google.common.base.Preconditions;
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
 * Nadeef Thrift client. This is the wrapper class for calling thrift methods.
 *
 *
 */
public final class NadeefClient {
    private static NadeefClient instance;
    private static String url;
    private static int port;

    /**
     * Initialize the Client params.
     * @param url_ remote url.
     * @param port_ port number.
     */
    public static void initialize(String url_, int port_) {
        Preconditions.checkArgument(url == null && port == 0);
        url = Preconditions.checkNotNull(url_);
        port = port_;
    }

    /**
     * Gets the instance of client.
     * @return instance.
     */
    public synchronized static NadeefClient getInstance() throws TTransportException {
        Preconditions.checkNotNull(url);
        if (instance == null) {
            instance = new NadeefClient();
        }
        return instance;
    }

    private NadeefClient() {}

    @SuppressWarnings("unchecked")
    public String generate(
        String type,
        String name,
        String code,
        String table1
    ) throws TException {
        TTransport transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TNadeefService.Client client = new TNadeefService.Client(protocol);

        TRule rule = new TRule(name, type, code);
        JSONObject result = new JSONObject();
        String gen = client.generate(rule, table1);
        result.put("data", gen);

        transport.close();
        return result.toJSONString();
    }

    @SuppressWarnings("unchecked")
    public String verify(String type, String name, String code) throws TException {
        TTransport transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TNadeefService.Client client = new TNadeefService.Client(protocol);

        JSONObject json = new JSONObject();
        boolean result = client.verify(new TRule(name, type, code));
        json.put("data", result);

        transport.close();
        return json.toJSONString();
    }

    @SuppressWarnings("unchecked")
    public String detect(
        String type,
        String name,
        String code,
        String table1,
        String table2
    ) throws TException {
        TTransport transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TNadeefService.Client client = new TNadeefService.Client(protocol);

        TRule rule = new TRule(name, type, code);
        JSONObject json = new JSONObject();
        String result = client.detect(rule, table1, table2);
        json.put("data", result);

        transport.close();
        return json.toJSONString();
    }

    @SuppressWarnings("unchecked")
    public String repair(
        String type,
        String name,
        String code,
        String table1,
        String table2
    ) throws TException {
        TTransport transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TNadeefService.Client client = new TNadeefService.Client(protocol);

        TRule rule = new TRule(name, type, code);
        JSONObject result = new JSONObject();
        result.put("data", client.repair(rule, table1, table2));

        transport.close();
        return result.toJSONString();
    }

    @SuppressWarnings("unchecked")
    public String getJobStatus() throws TException {
        TTransport transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TNadeefService.Client client = new TNadeefService.Client(protocol);

        JSONObject result = new JSONObject();
        List<TJobStatus> statusList = client.getAllJobStatus();
        JSONArray jsonArray = new JSONArray();

        for (TJobStatus status : statusList) {
            JSONObject obj = new JSONObject();
            obj.put("status", status.getStatus().toString());
            obj.put("overallProgress", status.getOverallProgress());
            obj.put("key", status.getKey());
            obj.put("progress", status.getProgress());
            obj.put("name", status.getNames());
            jsonArray.add(obj);
        }

        result.put("data", jsonArray);
        transport.close();
        return result.toJSONString();
    }
}
