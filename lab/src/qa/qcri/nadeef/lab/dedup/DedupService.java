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

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.tools.Tracer;

import static qa.qcri.nadeef.core.util.Bootstrap.shutdown;

public class DedupService extends AbstractIdleService {
    private TServer server;
    private static Tracer tracer = Tracer.getTracer(DedupService.class);

    @Override
    @SuppressWarnings("unchecked")
    protected void startUp() throws Exception {
        Bootstrap.start();
        int port = NadeefConfiguration.getServerPort();

        DedupServiceHandler handler = new DedupServiceHandler();
        TDedupService.Processor processor =
            new TDedupService.Processor(handler);
        TServerTransport serverTransport = new TServerSocket(port);
        server =
            new TThreadPoolServer(
                new TThreadPoolServer
                    .Args(serverTransport)
                    .processor(processor)
            );
        tracer.info("Starting NADEEF Dedup server @ " + port);
        server.serve();
    }

    @Override
    protected void shutDown() throws Exception {
        if (server != null && server.isServing()) {
            server.stop();
        }
        shutdown();
    }

    /**
     * Starts the NADEEF server.
     * @param args command line args.
     */
    public static void main(String[] args) {
        DedupService service = null;
        try {
            service = new DedupService();
            service.startUp();
            Thread.sleep(100);
        } catch (Exception ex) {
            tracer.err("Nadeef service has exception underneath.", ex);
            ex.printStackTrace();
        } finally {
            if (service != null) {
                try {
                    service.shutDown();
                } catch (Exception ex) {
                    // ignore
                }
            }
            shutdown();
        }
    }
}
