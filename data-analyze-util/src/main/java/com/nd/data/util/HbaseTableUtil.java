package com.nd.data.util;

import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author aladdin
 */
public class HbaseTableUtil {

    //产品用户表
    //列族
    public final static byte[] COLUMN_FAMILY = Bytes.toBytes("INFO");
    //imei
    public final static byte[] IMEI = Bytes.toBytes("imei");
    //uin
    public final static byte[] UIN = Bytes.toBytes("uin");
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
    //1日流失时间
    public final static byte[] LEAVE01_DATE = Bytes.toBytes("leave01Date");
    //7日流失时间
    public final static byte[] LEAVE07_DATE = Bytes.toBytes("leave07Date");
    //14日流失时间
    public final static byte[] LEAVE14_DATE = Bytes.toBytes("leave14Date");
    //30日流失时间
    public final static byte[] LEAVE30_DATE = Bytes.toBytes("leave30Date");
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
    //最后修改日期
    public final static byte[] LAST_UPDATE_DATE = Bytes.toBytes("lastUpdateDate");
}
