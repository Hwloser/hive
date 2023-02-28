package org.apache.hadoop.hive.ql.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class RedisUtil {
    public static JedisCluster jedisCluster = null;

    private RedisUtil() {
    }
    public static JedisCluster getJedisCluster() {
        Set<HostAndPort> nodes = new HashSet<>();
        String redisClusterNodes = SessionState.get().getConf().get("redis.cluster.nodes");
        String[] hostPortPeer = redisClusterNodes.split(",");
        for (String s : hostPortPeer) {
            String[] ss = s.split(":");
            nodes.add(new HostAndPort(ss[0], Integer.parseInt(ss[1])));
        }
        jedisCluster = new JedisCluster(nodes, 2000, 2000, 5, SessionState.get().getConf().get("redis.cluster.password"), new GenericObjectPoolConfig());
        return jedisCluster;
    }
}
