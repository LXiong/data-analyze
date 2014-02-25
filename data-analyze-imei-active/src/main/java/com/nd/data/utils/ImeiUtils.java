package com.nd.data.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA. User: Lin QiLi Date: 14-2-18 Time: 下午2:51
 */
public class ImeiUtils {

    /**
     * Byte array to HEX(String)
     *
     * @param byteArray byteArray
     * @return Hex(String)
     */
    private static String byteArrayToHex(byte[] byteArray) {
        String stamp;
        StringBuilder sb = new StringBuilder("");
        for (byte aByte : byteArray) {
            stamp = Integer.toHexString(aByte & 0xFF);
            sb.append((stamp.length() == 1) ? "0" + stamp : stamp);
        }
        return sb.toString().toLowerCase().trim();
    }

    /**
     * Generate rowKey like xxxx_product_imei. xxxx is the first four letters of
     * md5(imei)
     *
     * @param imei string imei
     * @param product string productId
     * @return string rowKey
     * @throws NoSuchAlgorithmException
     */
    public static String generateRowKey(String imei, String product) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(imei.getBytes());
        byte[] digest = messageDigest.digest();
        String md5 = byteArrayToHex(digest);

        md5 = md5.substring(0, 4);

        return md5 + "_" + product + "_" + imei;
    }

    /**
     * parse seconds since 1970-1-1 00:00:00 to yyyy-MM-dd format String
     *
     * @param dateTime seconds since 1970-1-1 00:00:00
     * @return yyyy-MM-dd format String
     */
    public static String parseSecondsToYyyyMmDd(String dateTime) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date(Long.parseLong(dateTime) * 1000);
        return dateFormat.format(date);
    }

    /**
     * calculate the interval days between begin and end Example: if begin =
     * 1980-1-1, end = 1980-1-2, then return 1.
     *
     * @param begin yyyy-MM-dd format string
     * @param end yyyy-MM-dd format string
     * @return long intervals
     * @throws ParseException
     */
    public static long calcIntervalDays(String begin, String end) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date beginDate = dateFormat.parse(begin);
        Date endDate = dateFormat.parse(end);
        return (endDate.getTime() - beginDate.getTime()) / 864000;
    }
}