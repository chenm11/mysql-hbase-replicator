package mysql2hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.collection.mutable.ArrayBuffer;
import javax.management.*;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by CM on 2015/8/26.
 *
 */
public class StatusServlet extends HttpServlet {
    protected transient MBeanServer mBeanServer = null;
    private static final Log LOG = LogFactory.getLog(StatusServlet.class);
    @Override
    public void init() throws ServletException {
        // Retrieve the MBean server
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        PrintWriter writer = resp.getWriter();
        resp.setContentType("text/html; charset=utf8");
        writer.append("<!DOCTYPE html>");
        writer.append("<html>");
        writer.append("<title>mysql to hbase replication</title>");
        writer.append("<body>");
        try {
            writer.append(toHtmlTable(new ObjectName("com.cm:type=mysql2hbase.hbaseApplier")));
            writer.append(toHtmlTable(new ObjectName("com.cm:type=mysql2hbase.mySQLExtractor")));

        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (ReflectionException e) {
            e.printStackTrace();
        } catch (InstanceNotFoundException e) {
            e.printStackTrace();
        } catch (IntrospectionException e) {
            e.printStackTrace();
        } catch (AttributeNotFoundException e) {
            e.printStackTrace();
        } catch (MBeanException e) {
            e.printStackTrace();
        }
        writer.append("</body></html>");
    }

    String toHtmlTable(ObjectName oname) throws IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        MBeanInfo minfo = mBeanServer.getMBeanInfo(oname);
        StringBuffer sb = new StringBuffer();
        for (MBeanAttributeInfo info:minfo.getAttributes()){
            String html = toHtmlTable(oname,info);
            sb.append(html);
        }
        return sb.toString();
    }


    String toHtmlTable(ObjectName oname,MBeanAttributeInfo attr) throws IntrospectionException, InstanceNotFoundException, ReflectionException, AttributeNotFoundException, MBeanException {
        String name =  attr.getName();
        Object value = mBeanServer.getAttribute(oname, name);
        StringBuffer sb = new StringBuffer();
        if(name.equals("Binlog")){
            sb.append("<hr><table border=1 cellpadding=5 cellspacing=0><thead ><tr><td><b> mysql binlog file</b></td></tr></thead>");
            sb.append("<thead><tr><td>"+value.toString()+"</td></tr></thead></table>");

        }else if(name.equals("BinlogPosition")){
            sb.append("<hr><table border=1 cellpadding=5 cellspacing=0><thead ><tr><td><b> mysql binlog position </b></td></tr></thead>");
            sb.append("<thead><tr><td>"+value.toString()+"</td></tr></thead></table>");
        }else if(name.equals("Count")){
            HashMap<String,AtomicLong> cds = (HashMap<String,AtomicLong>)value;
            Set<String> keys = cds.keySet();
            sb.append("<hr><table border=1 cellpadding=5 cellspacing=0><thead ><tr><td><b>mysql action type </b></td><td><b>count</b></td></tr></thead>");
            for(String key: keys) {
                sb.append("<thead><tr><td>" + key + "</td><td>" + cds.get(key) + "</td></tr></thead>");
            }
            sb.append("</table>");
        }else if(name.equals("Delay")){
            HashMap<String,ArrayBuffer<Long>> cds = (HashMap<String,ArrayBuffer<Long>>)value;
            Set<String> keys = cds.keySet();
            sb.append("<hr><table border=1 cellpadding=5 cellspacing=0><thead ><tr><td><b>hbase action type </b></td><td><b>delay (ms) </b></td></tr></thead>");
            for(String key: keys) {
                ArrayBuffer<Long> tds = cds.get(key);
                sb.append("<thead><tr><td>" + key + "</td><td>" +tds.mkString(",") + "</td></tr></thead>");
            }
            sb.append("</table>");
        }else if(name.equals("TableEventCount")){

        }
        return sb.toString();
    }



}
