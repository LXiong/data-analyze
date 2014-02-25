package com.nd.data.utils;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Lin QiLi
 * Date: 14-2-18
 * Time: 下午2:17
 */
public class ImeiHBaseUtils {
    private static Logger _LOG = Logger.getLogger(ImeiHBaseUtils.class);

    private final static byte[] CF_BYTES = Bytes.toBytes("INFO");
    private final static byte[] PRODUCT_BYTES = Bytes.toBytes("product");
    private final static byte[] PLATFORM_BYTES = Bytes.toBytes("platForm");
    private final static byte[] CHANNEL_ID_BYTES = Bytes.toBytes("channelId");
    private final static byte[] PRODUCT_VERSION_BYTES = Bytes.toBytes("productVersion");
    private final static byte[] FIRST_DATE_BYTES = Bytes.toBytes("firstDate");
    private final static byte[] LAST_DATE_BYTES = Bytes.toBytes("lastDate");
    private final static byte[] IMEI_BYTES = Bytes.toBytes("imei");
    private final static byte[] LOGIN_CNT_BYTES = Bytes.toBytes("loginCnt");
    private final static byte[] LEAVE01DATE = Bytes.toBytes("leave01Date");
    private final static byte[] LEAVE01CNT = Bytes.toBytes("leave01Cnt");
    private final static byte[] LEAVE07DATE = Bytes.toBytes("leave07Date");
    private final static byte[] LEAVE07CNT = Bytes.toBytes("leave07Cnt");
    private final static byte[] LEAVE14DATE = Bytes.toBytes("leave14Date");
    private final static byte[] LEAVE14CNT = Bytes.toBytes("leave14Cnt");
    private final static byte[] LEAVE30DATE = Bytes.toBytes("leave30Date");
    private final static byte[] LEAVE30CNT = Bytes.toBytes("leave30Cnt");
    private final static byte[] LAST_UPDATE_DATE_BYTES = Bytes.toBytes("lastUpdateDate");
    private final static byte[] NULL_STRING = Bytes.toBytes("");
    private final static List<byte[]> otherQualifierList = ImmutableList.of(
            LEAVE01DATE,
            LEAVE01CNT,
            LEAVE07DATE,
            LEAVE07CNT,
            LEAVE14DATE,
            LEAVE14CNT,
            LEAVE30DATE,
            LEAVE30CNT
    );

    /**
     * query hbase.
     * @param hTable htable
     * @param columnFamily cf
     * @param rowKey rowkey
     * @return Result or null.
     * @throws IOException
     */
    public static Result queryByRowKey(HTable hTable, String columnFamily, String rowKey) throws IOException {
        byte[] bytesRowKey = Bytes.toBytes(rowKey);

        Get g = new Get(bytesRowKey);
        g.addFamily(Bytes.toBytes(columnFamily));
        Result rs = hTable.get(g);
        if (!rs.isEmpty()) {
            _LOG.info("queryByRowKey rowKey: "+ rowKey + " CF_BYTES: "+columnFamily + " result: "+rs);
            return rs;
        }else {
            return null;
        }
    }

    /**
     * create insert input.
     * @param rowKey rowkey
     * @param imeiProductUser imeiProductUser
     * @return Put put or null.
     */
    public static Put createInsertPut(String rowKey, ImeiProductUser imeiProductUser) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.setWriteToWAL(false);
        String[] record = rowKey.split("_");
        String imei = record[2];
        if (!imei.equals("")
                && !imeiProductUser.get_platForm().equals("")
                && !imeiProductUser.get_product().equals("")) {
            put.add(CF_BYTES, IMEI_BYTES, Bytes.toBytes(imei));
            put.add(CF_BYTES, PRODUCT_BYTES, Bytes.toBytes(imeiProductUser.get_product()));
            put.add(CF_BYTES, PLATFORM_BYTES, Bytes.toBytes(imeiProductUser.get_platForm()));
            put.add(CF_BYTES, CHANNEL_ID_BYTES, Bytes.toBytes(imeiProductUser.get_channelId()));
            put.add(CF_BYTES, PRODUCT_VERSION_BYTES, Bytes.toBytes(imeiProductUser.get_productVersion()));
            put.add(CF_BYTES, FIRST_DATE_BYTES, Bytes.toBytes(imeiProductUser.get_firstDay()));
            put.add(CF_BYTES, LAST_DATE_BYTES, Bytes.toBytes((imeiProductUser.get_lastDay())));
            put.add(CF_BYTES, LOGIN_CNT_BYTES, Bytes.toBytes(imeiProductUser.get_loginCnt()));
            put.add(CF_BYTES, LAST_UPDATE_DATE_BYTES, Bytes.toBytes(imeiProductUser.get_lastDay())); // modify the last update date.
            // other qualifiers set ""
            for (byte[] qualifier : otherQualifierList) {
                put.add(CF_BYTES, qualifier, NULL_STRING);
            }
            return put;
        } else {
            return null;
        }
    }

    /**
     * Create update put
     * @param rowKey rowkey
     * @param imeiProductUser input class
     * @return Put put
     */
    public static Put createUpdatePut(String rowKey, ImeiProductUser imeiProductUser) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.setWriteToWAL(false);
        put.add(CF_BYTES, LAST_DATE_BYTES, Bytes.toBytes(imeiProductUser.get_lastDay()));
        put.add(CF_BYTES, PRODUCT_VERSION_BYTES, Bytes.toBytes(imeiProductUser.get_productVersion()));
        put.add(CF_BYTES, LOGIN_CNT_BYTES, Bytes.toBytes(imeiProductUser.get_loginCnt()));
        put.add(CF_BYTES, LAST_UPDATE_DATE_BYTES, Bytes.toBytes(imeiProductUser.get_lastDay())); // modify the last update date.
        return put;
    }
}