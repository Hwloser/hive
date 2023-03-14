package org.apache.hive.jdbc;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.junit.Test;

public class UtilsTest {
  @Test
  public void testConnection() throws ZooKeeperHiveClientException, SQLException {
    String url = "jdbc:hive2://hzpl004138023-hadoop-zh.ztosys.com:2181,hzpl004138024-hadoop-zh.ztosys.com:2181,hzpl004138041-hadoop-zh.ztosys.com:2181,hzpl004138042-hadoop-zh.ztosys.com:2181,hzpl004138051-hadoop-zh.ztosys.com:2181/;\r" +
        "serviceDiscoveryMode=zooKeeper;" +
        "zooKeeperNamespace=ztohive2";
//    JdbcConnectionParams params = Utils.parseURL(url);
//    System.out.println(getAuthorities(url, null));

//    String url2 = "jdbc:hive2://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2#k3=v3";
//    System.out.println("--tt-------");
//    System.out.println(getAuthorities(url2, null));
//
//    System.out.println("--tt-------");

//    System.out.println(Utils.parseURL(url2));
//    System.out.println("--tt-------");
//
//    URI uri = URI.create(url.substring("jdbc:".length()));
//    System.out.println(uri);

    String uri2 = "jdbc:hive2://hzpl004138023-hadoop-zh.ztosys.com:2181,hzpl004138024-hadoop-zh.ztosys.com:2181,hzpl004138041-hadoop-zh.ztosys.com:2181,hzpl004138042-hadoop-zh.ztosys.com:2181,hzpl004138051-hadoop-zh.ztosys.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=ztohive2";
//    URI uri = URI.create(uri2.substring("jdbc:".length()));
//    System.out.println(uri);
    Utils.parseURL(uri2);
  }

  private static String getAuthorities(String uri, JdbcConnectionParams connParams)
      throws JdbcUriParseException {
    String authorities;
    /**
     * For a jdbc uri like:
     * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?conf_list#var_list
     * Extract the uri host:port list starting after "jdbc:hive2://",
     * till the 1st "/" or "?" or "#" whichever comes first & in the given order
     * Examples:
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3#k3=v3
     */
    int fromIndex = Utils.URL_PREFIX.length();
    int toIndex = -1;
    ArrayList<String> toIndexChars = new ArrayList<String>(Arrays.asList("/", "?", "#"));
    for (String toIndexChar : toIndexChars) {
      toIndex = uri.indexOf(toIndexChar, fromIndex);
      if (toIndex > 0) {
        break;
      }
    }
    if (toIndex < 0) {
      authorities = uri.substring(fromIndex);
    } else {
      authorities = uri.substring(fromIndex, toIndex);
    }
    return authorities;
  }
}