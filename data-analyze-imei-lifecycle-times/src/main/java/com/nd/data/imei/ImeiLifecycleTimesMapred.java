package com.nd.data.imei;

import com.nd.mapred.PartitionUtils;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static com.nd.data.util.HbaseTableUtil.*;
import com.nd.data.util.LeaveTypeEnum;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author aladdin
 */
public class ImeiLifecycleTimesMapred {

    //统计日期参数名
    public final static String STATE_DATE_NAME = "stateDate";
    //
    public final static String TABLE_NAME = "tableName";

    public static class MyMapper extends TableMapper<Text, Text> {

        //统计时间
        private String stateDate;
        //产品用户总表
        private HTable hTable;
        //日期处理
        private SimpleDateFormat dateFormatYYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");
        //map输出
        private final Text newKey = new Text();
        private final Text newValue = new Text();
        //间隔天数集合
        private final int[] days = {1, 2, 3, 4, 5, 6, 7, 14, 21, 30};
        private final String[] fields = {
            "imei", "firstDate", "lastDate", "product", "channelId", "platForm",
            "productVersion", "loginCnt"
        };

        /**
         * mapper 初始化
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.stateDate = context.getConfiguration().get(STATE_DATE_NAME);
            String tableName = context.getConfiguration().get(TABLE_NAME);
            this.hTable = new HTable(context.getConfiguration(), tableName);
//            this.hTable.setAutoFlush(false);
//            this.hTable.setWriteBufferSize(64 * 1024 * 1024);
        }

        /**
         * 计算两个日期间隔天数
         *
         * @param startDate
         * @param endDate
         * @return
         */
        private int getIntervalDays(String startDate, String endDate) {
            int result = 1;
            try {
                Date start = this.dateFormatYYYYMMDD.parse(startDate);
                Date end = this.dateFormatYYYYMMDD.parse(endDate);
                long interval = end.getTime() - start.getTime();
                if (interval >= 0) {
                    result = (int) (interval / 86400000) + 1;
                }
            } catch (ParseException ex) {
                System.out.println("error date value:" + startDate + " " + endDate);
            }
            return result;
        }

        private void updateLeaveCnt(byte[] leaveField, String product, String imei, String loginCnt, String lastUpdateDate) throws IOException {
            String part = PartitionUtils.getPartition(product);
            StringBuilder keyBuilder = new StringBuilder(36);
            keyBuilder.append(part).append('_').append(product).append('_').append(imei);
            String rowKey = keyBuilder.toString();
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(COLUMN_FAMILY, leaveField, Bytes.toBytes(loginCnt));
            put.add(COLUMN_FAMILY, LAST_UPDATE_DATE, Bytes.toBytes(lastUpdateDate));
            this.hTable.put(put);
        }

        /**
         * 读取hbase产品用户总表内容，输出统计中间结果
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            if (value != null) {
                Map<String, String> fieldValueMap = new HashMap<String, String>(this.fields.length, 1);
                boolean flag = true;
                //取值，并验证记录是否合法
                String fieldName;
                String fieldValue;
                for (int index = 0; index < this.fields.length; index++) {
                    fieldName = this.fields[index];
                    fieldValue = Bytes.toString(value.getValue(COLUMN_FAMILY, Bytes.toBytes(fieldName)));
                    if (fieldValue == null) {
                        //字段为null，该记录无效
                        flag = false;
                        break;
                    } else {
                        fieldValueMap.put(fieldName, fieldValue);
                    }
                }
                if (flag) {
                    //有效记录
                    final String imei = fieldValueMap.get("imei");
                    final String firstDate = fieldValueMap.get("firstDate");
                    final String lastDate = fieldValueMap.get("lastDate");
                    final String product = fieldValueMap.get("product");
                    final String channelId = fieldValueMap.get("channelId");
                    final String platForm = fieldValueMap.get("platForm");
                    final String productVersion = fieldValueMap.get("productVersion");
                    final String loginCnt = fieldValueMap.get("loginCnt");
                    //遍历所有流失统计方式
                    String leaveType;
                    int maxInterval;
                    byte[] leaveField;
                    String leaveCnt;
                    int loginTimes;
                    for (LeaveTypeEnum leaveTypeEnum : LeaveTypeEnum.values()) {
                        leaveType = leaveTypeEnum.getLeaveType();
                        leaveField = leaveTypeEnum.getLeaveCntField();
                        leaveCnt = Bytes.toString(value.getValue(COLUMN_FAMILY, leaveField));
                        //计算登录天次
                        if (leaveCnt == null || leaveCnt.isEmpty()) {
                            //未流失，计算流失间隔，为lastDate到stateDate的自然日跨度天数
                            int leaveDays = this.getIntervalDays(lastDate, this.stateDate);
                            //判断是否流失
                            maxInterval = leaveTypeEnum.getMaxInterval();
                            if (leaveDays <= maxInterval) {
                                //当前未流失
                                //登录天次为该用户累计登录天次
                                loginTimes = Integer.parseInt(loginCnt);
                            } else {
                                //当前首次流失
                                //更新当前流失类型的登录天次为该用户累计登录天次
                                this.updateLeaveCnt(leaveField, product, imei, loginCnt, this.stateDate);
                                //登录天次为该用户累计登录天次
                                loginTimes = Integer.parseInt(loginCnt);
                            }
                        } else {
                            //已流失，登录天次就是leaveCnt
                            loginTimes = Integer.parseInt(leaveCnt);
                        }
                        //将间隔天数转换成对应的间隔天数统计集合
                        final StringBuilder keyBuilder = new StringBuilder(128);
                        this.newValue.set(imei);
                        for (int index = 0; index < this.days.length; index++) {
                            if (this.days[index] <= loginTimes) {
                                keyBuilder.append(firstDate).append('\t').append(this.days[index]).append('\t')
                                        .append(leaveType).append('\t').append(product).append('\t')
                                        .append(channelId).append('\t').append(platForm).append('\t')
                                        .append(productVersion);
                                this.newKey.set(keyBuilder.toString());
                                context.write(this.newKey, this.newValue);
                                keyBuilder.setLength(0);
                            } else {
                                break;
                            }
                        }

                    }
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        private final Text newKey = new Text();
        private final Text newValue = new Text();

        /**
         * 获取mapper的输出信息，分组计算后输出用户生命周期统计的结果
         *
         * @param key
         * firstDate,loginTimes,leaveType,product,channelId,platForm,productVersion,列之前用\t间隔
         * @param values imei的集合
         * @param context
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //timesCnt为values的size
            int count = 0;
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                count++;
                iterator.next();
            }
            StringBuilder keyBuilder = new StringBuilder(128);
            keyBuilder.append(key.toString()).append('\t').append(count);
            this.newKey.set(keyBuilder.toString());
            context.write(this.newKey, this.newValue);
        }
    }
}
