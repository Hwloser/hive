package org.apache.hadoop.hive.ql.util;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * create by dijun on 2022/2/17
 */
public class NotifyUtil {

    private static final Log LOG = LogFactory.getLog(NotifyUtil.class);

    /**
     * 记录hive异常信息
     *
     * @param ss
     */
    public static void record(SessionState ss, String errorMsg, String sql) {
        String jobId = ss.getConf().get("mapreduce.job.id");
        String failedMsg = ss.getConf().get("mapreduce.job.failed.message");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("isOnYarn", StringUtils.isNotBlank(jobId));
        map.put("sqlMd5", ss.getConf().get(HiveConf.ConfVars.HIVEQUERYID.varname));
        map.put("sql", sql);
        map.put("startTime", ss.getConf().get("query.start.time"));
        map.put("endTime", new Date().getTime());
        map.put("jobName", ss.getConf().get("mapred.job.name"));
        map.put("jobId", jobId);
        map.put("type", "hive");
        map.put("user", ss.getUserName());
        map.put("exception", failedMsg == null ? errorMsg : failedMsg);
        String queryInfo = new Gson().toJson(map);
        LOG.info(queryInfo);
        RedisUtil.getJedisCluster().lpush("ht:collect:sql:final:failed:info", queryInfo);
    }
}
