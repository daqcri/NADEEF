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

package qa.qcri.nadeef.lab.dedup;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONObject;

import java.util.List;

public class DedupClient {
    private static DedupClient instance;
    private static String url;
    private static int port;

    /**
     * Initialize the Client params.
     * @param url_ remote url.
     * @param port_ port number.
     */
    public static void initialize(String url_, int port_) {
        url = Preconditions.checkNotNull(url_);
        port = port_;
    }

    /**
     * Gets the instance of client.
     * @return instance.
     */
    public synchronized static DedupClient getInstance() throws TTransportException {
        Preconditions.checkNotNull(url);
        if (instance == null) {
            instance = new DedupClient();
        }
        return instance;
    }

    private DedupClient() {}

    public String incrementalDedup(List<Integer> newItems) throws TException {
        TTransport transport = new TSocket(url, port);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TDedupService.Client client = new TDedupService.Client(protocol);
        JSONObject json = new JSONObject();
        List<List<Integer>> result = client.incrementalDedup(newItems);
        json.put("data", result);
        transport.close();
        return json.toJSONString();
    }
}
