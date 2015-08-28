package mysql2hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Created by CM on 2015/8/26.
 *
 */
public class HttpServer implements Runnable {
    public static final Log LOG = LogFactory.getLog(HttpServer.class);
    protected final Server webServer;

    public HttpServer() {
        webServer= new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(31081);
        connector.setMaxIdleTime(30000);
        connector.setRequestHeaderSize(8192);
        QueuedThreadPool threadPool =  new QueuedThreadPool(5);
        threadPool.setName("embed-jetty-http");
        connector.setThreadPool(threadPool);
        webServer.setConnectors(new Connector[]{connector});
        ServletContextHandler context =new ServletContextHandler(webServer, "/");
        context.addServlet(JmxServlet.class, "/jmx");
        context.addServlet(StatusServlet.class, "/");
        LOG.info("jmx rest server init completed");
    }


    @Override
    public void run() {
        try {
            webServer.start();
        } catch (Exception e) {
            LOG.info(e);
        }
    }
}
