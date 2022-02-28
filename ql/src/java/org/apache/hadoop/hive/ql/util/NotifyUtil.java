package org.apache.hadoop.hive.ql.util;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * create by dijun on 2022/2/17
 */
public class NotifyUtil {

    private static final Log LOG = LogFactory.getLog(NotifyUtil.class);

    private static final String MR = "MAPREDUCE";

    private static final Gson GSON = new Gson();

    /**
     * 记录hive异常信息
     *
     * @param ss
     */
    public static void record(SessionState ss, String errorMsg, String sql) {
        // appName
        String appName = ss.getConf().get("mapred.job.name");
        // jobId
        String jobId = ss.getConf().get("mapreduce.job.id");
        // 异常信息
        String failedMsg = ss.getConf().get("mapreduce.job.failed.message");
        // 查询平台queryId
        String queryString = ss.getConf().get("hive.query.string");
        // 回调url，从配置文件获取
        String notifyUrl = ss.getConf().get("root.exception.notify.url");
        // 发送回调请求
        RootCauseReq req = new RootCauseReq(MR, jobId, appName, failedMsg == null ? errorMsg : failedMsg, queryString, sql);
        try {
            post(notifyUrl, GSON.toJson(req));
        } catch (Exception e) {
            LOG.error("发送通知失败，失败原因: " + e.getMessage());
        }
    }

    /**
     * 发送post请求
     *
     * @param url
     * @param msg
     * @throws Exception
     */
    private static void post(String url, String msg) throws Exception {
        LOG.info("开始推送错误: " + msg);
        HttpURLConnection con = null;
        try {
            URL obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();

            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");

            //发送Post请求
            con.setDoOutput(true);
            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.write(msg.getBytes(StandardCharsets.UTF_8));
                wr.flush();
            }
            String result = con.getResponseMessage();
            LOG.info("错误推送结果：" + result);
        } finally {
            if (con != null) {
                con.disconnect();
            }
        }
    }

    /**
     * 请求实体类
     */
    private static class RootCauseReq {

        // 应用类型
        private String appType;

        // 应用id
        private String applicationId;

        // 应用名
        private String name;

        // 异常信息
        private String exception;

        // queryString
        private String queryString;

        private String sql;

        public RootCauseReq(String appType, String applicationId, String name, String exception, String queryString, String sql) {
            this.appType = appType;
            this.applicationId = applicationId;
            this.name = name;
            this.exception = exception;
            this.queryString = queryString;
            this.sql = sql;
        }

        public RootCauseReq() {
        }

        public String getAppType() {
            return appType;
        }

        public void setAppType(String appType) {
            this.appType = appType;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public void setApplicationId(String applicationId) {
            this.applicationId = applicationId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getException() {
            return exception;
        }

        public void setException(String exception) {
            this.exception = exception;
        }

        public String getQueryString() {
            return queryString;
        }

        public void setQueryString(String queryString) {
            this.queryString = queryString;
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        @Override
        public String toString() {
            return "{\n" +
                    "    \"appType\": \"" + appType + "\",\n" +
                    "    \"applicationId\": \"" + applicationId + "\",\n" +
                    "    \"exception\": \"" + exception + "\",\n" +
                    "    \"name\": \"" + name + "\",\n" +
                    "    \"queryString\": \"" + queryString + "\",\n" +
                    "    \"sql\": \"" + sql + "\"\n" +
                    "}";
        }
    }

}
