package com.nd.data.imei.lifecycle.times;

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

/**
 *
 * @author aladdin
 */
public class ImeiLifecycleTimesMapred {

    //统计日期参数名
    public final static String STATE_DATE_NAME = "stateDate";
    //
    public final static String TABLE_NAME = "tableName";
    //产品用户表
    //列族
    public final static byte[] COLUMN_FAMILY = Bytes.toBytes("INFO");
    //imei
    public final static byte[] IMEI = Bytes.toBytes("imei");
    //首次登录时间
    public final static byte[] FIRST_DATE = Bytes.toBytes("firstDate");
    //最后登录时间
    public final static byte[] LAST_DATE = Bytes.toBytes("lastDate");
    //产品ID
    public final static byte[] PRODUCT = Bytes.toBytes("product");
    //渠道ID
    public final static byte[] CHANNEL_ID = Bytes.toBytes("channelId");
    //平台
    public final static byte[] PLAT_FORM = Bytes.toBytes("platForm");
    //产品版本
    public final static byte[] PRODUCT_VERSION = Bytes.toBytes("productVersion");
    //累计登录天次
    public final static byte[] LOGIN_CNT = Bytes.toBytes("loginCnt");
    //1日流失时间
    public final static byte[] LEAVE01_CNT = Bytes.toBytes("leave01Cnt");
    //7日流失时间
    public final static byte[] LEAVE07_CNT = Bytes.toBytes("leave07Cnt");
    //14日流失时间
    public final static byte[] LEAVE14_CNT = Bytes.toBytes("leave14Cnt");
    //30日流失时间
    public final static byte[] LEAVE30_CNT = Bytes.toBytes("leave30Cnt");

    /**
     * 流失类型枚举
     */
    private static enum LeaveTypeEnum {

        LEAVE01,
        LEAVE07,
        LEAVE14,
        LEAVE30
    }

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
         * 构造map out key
         *
         * @param typeEnum
         * @param firstDate
         * @param product
         * @param platForm
         * @param channelId
         * @param productVersion
         * @param loginTimes
         * @return
         */
        private String createMapOutKey(LeaveTypeEnum typeEnum, String firstDate, String product, String platForm, String channelId, String productVersion, String loginTimes) {
            String leaveType;
            switch (typeEnum) {
                case LEAVE01:
                    leaveType = "leave01";
                    break;
                case LEAVE07:
                    leaveType = "leave07";
                    break;
                case LEAVE14:
                    leaveType = "leave14";
                    break;
                case LEAVE30:
                    leaveType = "leave30";
                    break;
                default:
                    leaveType = "leave30";
            }
            final StringBuilder keyBuilder = new StringBuilder(128);
            keyBuilder.append(firstDate).append('\t').append(loginTimes).append('\t')
                    .append(leaveType).append('\t').append(product).append('\t')
                    .append(channelId).append('\t').append(platForm).append('\t')
                    .append(productVersion);
            return keyBuilder.toString();
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

        private void updateLeaveCnt(LeaveTypeEnum typeEnum, String product, String imei, String loginCnt) throws IOException {
            String part = PartitionUtils.getPartition(product);
            StringBuilder keyBuilder = new StringBuilder(36);
            keyBuilder.append(part).append('_').append(product).append('_').append(imei);
            String rowKey = keyBuilder.toString();
            Put put = new Put(Bytes.toBytes(rowKey));
            switch (typeEnum) {
                case LEAVE01:
                    put.add(COLUMN_FAMILY, LEAVE01_CNT, Bytes.toBytes(loginCnt));
                    break;
                case LEAVE07:
                    put.add(COLUMN_FAMILY, LEAVE07_CNT, Bytes.toBytes(loginCnt));
                    break;
                case LEAVE14:
                    put.add(COLUMN_FAMILY, LEAVE14_CNT, Bytes.toBytes(loginCnt));
                    break;
                case LEAVE30:
                    put.add(COLUMN_FAMILY, LEAVE30_CNT, Bytes.toBytes(loginCnt));
                    break;
                default:
                    put.add(COLUMN_FAMILY, LEAVE30_CNT, Bytes.toBytes(loginCnt));
            }
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
                final String imei = Bytes.toString(value.getValue(COLUMN_FAMILY, IMEI));
                final String firstDate = Bytes.toString(value.getValue(COLUMN_FAMILY, FIRST_DATE));
                final String lastDate = Bytes.toString(value.getValue(COLUMN_FAMILY, LAST_DATE));
                final String product = Bytes.toString(value.getValue(COLUMN_FAMILY, PRODUCT));
                final String channelId = Bytes.toString(value.getValue(COLUMN_FAMILY, CHANNEL_ID));
                final String platForm = Bytes.toString(value.getValue(COLUMN_FAMILY, PLAT_FORM));
                final String productVersion = Bytes.toString(value.getValue(COLUMN_FAMILY, PRODUCT_VERSION));
                final String loginCnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LOGIN_CNT));
                final String leave01Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE01_CNT));
                final String leave07Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE07_CNT));
                final String leave14Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE14_CNT));
                final String leave30Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE30_CNT));
                //计算流失天数，流失天数为lastDate到stateDate的自然日跨度天数
                final int leaveDays = this.getIntervalDays(lastDate, this.stateDate);
                String keyValue;
                //判断用户是否已1日流失
                if (leave01Cnt.isEmpty()) {
                    //1日未流失
                    //判断用户是在stateDate首次1日流失
                    if (leaveDays <= 1) {
                        //用户在stateDate未1日流失
                        keyValue = this.createMapOutKey(
                                LeaveTypeEnum.LEAVE01,
                                firstDate,
                                product,
                                platForm,
                                channelId,
                                productVersion,
                                loginCnt);
                        this.newKey.set(keyValue);
                        this.newValue.set(imei);
                        context.write(this.newKey, this.newValue);
                    } else {
                        //用户在stateDate首次1日流失，更新流失登录天次
                        this.updateLeaveCnt(LeaveTypeEnum.LEAVE01, product, imei, loginCnt);
                    }
                }
                //判断用户是否已7日流失
                if (leave07Cnt.isEmpty()) {
                    //7日未流失
                    //判断用户是在stateDate首次7日流失
                    if (leaveDays <= 7) {
                        //用户在stateDate未1日流失
                        keyValue = this.createMapOutKey(
                                LeaveTypeEnum.LEAVE07,
                                firstDate,
                                product,
                                platForm,
                                channelId,
                                productVersion,
                                loginCnt);
                        this.newKey.set(keyValue);
                        this.newValue.set(imei);
                        context.write(this.newKey, this.newValue);
                    } else {
                        //用户在stateDate首次7日流失，更新流失登录天次
                        this.updateLeaveCnt(LeaveTypeEnum.LEAVE07, product, imei, loginCnt);
                    }
                }
                //判断用户是否已14日流失
                if (leave14Cnt.isEmpty()) {
                    //14日未流失
                    //判断用户是在stateDate首次14日流失
                    if (leaveDays <= 14) {
                        //用户在stateDate未14日流失
                        keyValue = this.createMapOutKey(
                                LeaveTypeEnum.LEAVE14,
                                firstDate,
                                product,
                                platForm,
                                channelId,
                                productVersion,
                                loginCnt);
                        this.newKey.set(keyValue);
                        this.newValue.set(imei);
                        context.write(this.newKey, this.newValue);
                    } else {
                        //用户在stateDate首次14日流失，更新流失登录天次
                        this.updateLeaveCnt(LeaveTypeEnum.LEAVE14, product, imei, loginCnt);
                    }
                }
                //判断用户是否已30日流失
                if (leave30Cnt.isEmpty()) {
                    //30日未流失
                    //判断用户是在stateDate首次30日流失
                    if (leaveDays <= 30) {
                        //用户在stateDate未30日流失
                        keyValue = this.createMapOutKey(
                                LeaveTypeEnum.LEAVE30,
                                firstDate,
                                product,
                                platForm,
                                channelId,
                                productVersion,
                                loginCnt);
                        this.newKey.set(keyValue);
                        this.newValue.set(imei);
                        context.write(this.newKey, this.newValue);
                    } else {
                        //用户在stateDate首次30日流失，更新流失登录天次
                        this.updateLeaveCnt(LeaveTypeEnum.LEAVE30, product, imei, loginCnt);
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
