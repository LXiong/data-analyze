package com.nd.data;

import com.nd.data.utils.ImeiHBaseUtils;
import com.nd.data.utils.ImeiProductUser;
import com.nd.data.utils.ImeiUtils;
import com.nd.mapred.AbstractJobStart;
import com.nd.mapred.sort.StringASCSortComparator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA. User: Lin QiLi Date: 14-2-18 Time: 下午2:15
 */
public class ImeiDailyActiveUser {

    private final static int MAX_NEW_KEY = 400;      // 32 + 64
    private final static int MAX_NEW_VALUE = 400;

    /**
     *
     */
    public static class MapperProcess extends Mapper<Object, Text, Text, Text> {

        // UserLoginLogInput
        //private String _uin;                // 0. UIN **not null**
        private String _platForm;           // 1. platForm can be null
        private String _product;            // 2. productID **not null**
        private String _productVersion;     // 3. product version can be null
        private String _imei;               // 4. imei can be null
        private String _channelId;          // 5. channel ID can be null
        private String _loginTime;          // 6. LoginTime **not null**
        private Text _newKey = new Text();
        private Text _newValue = new Text();
        StringBuilder _keyBuilder = new StringBuilder(MAX_NEW_KEY);
        StringBuilder _valueBuilder = new StringBuilder(MAX_NEW_VALUE);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] record = line.split("\t", 7); //Got 7 columns, each column could be null string.
            // Whether a complete line or not.
            // imei should not be null string.
            if (record.length == 7 && !record[4].equals("")) {
                //_uin = record[0];
                _platForm = record[1];
                _product = record[2];
                _productVersion = record[3];
                _imei = record[4];
                _channelId = record[5];
                _loginTime = record[6];


                // make new key
                _keyBuilder.append(_product).append("\t").append(_imei);
                String strKey = _keyBuilder.toString();
                _newKey.set(strKey);
                //Text newKey = new Text(_keyBuilder.toString());
                _keyBuilder.setLength(0);
                // make new value
                _valueBuilder.append(_loginTime).append("\t")
                        .append(_platForm).append("\t")
                        .append(_productVersion).append("\t")
                        .append(_channelId);
                //Text newValue = new Text(_valueBuilder.toString());
                _newValue.set(_valueBuilder.toString());
                _valueBuilder.setLength(0);
                // write to context
                context.write(_newKey, _newValue);
            }
        }
    }

    // combiner
    public static class CombinerProcess extends Reducer<Text, Text, Text, Text> {
        // KEY

        private String _product;            // 0. productID
        private String _imei;               // 1. imei
        // VALUE
        private String _loginTime;          // 0. LoginTime. seconds since 1970-1-1 00:00:00
        private String _platForm;           // 1. platForm
        private String _productVersion;     // 2. product version
        private String _channelId;          // 3. channel ID
        // New Key
        private String _firstDay;
        private String _intervalDays;
        private String _loginCnt;
        private String _loginDay;           // convert from loginTime
        private Text _newKey = new Text();
        private Text _newValue = new Text();
        // HTable
        private String _rowKey;
        private String _tableName;
        private HTable _hTable;
        private static final String CF = "INFO";
        private final static byte[] LOGIN_CNT_BYTES = Bytes.toBytes("loginCnt");
        private final static byte[] FIRST_DATE_BYTES = Bytes.toBytes("firstDate");
        private final static byte[] LAST_DATE_BYTES = Bytes.toBytes("lastDate");
        // others
        private List<String> _recordList = new ArrayList<String>();
        private String[] _record;
        private StringBuilder _keyBuilder = new StringBuilder(MAX_NEW_KEY);

        @Override
        public void setup(Context context) throws IOException {
            //初始化hTable处理对象，关闭自动提交，设置写入Buffer为64M
            _tableName = context.getConfiguration().get(AbstractJobStart.TABLE_NAME_PARA);
            _hTable = new HTable(context.getConfiguration(), _tableName);
            _hTable.setAutoFlush(false);
            _hTable.setWriteBufferSize(64 * 1024 * 1024);
            //this.startTime = System.currentTimeMillis();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Get product, imei from the key
            _record = key.toString().split("\t", 2);
            if (_record.length == 2) {
                _product = _record[0];
                _imei = _record[1];
                // Get the rest columns
                for (Text value : values) {
                    _recordList.add(value.toString());
                }
                //sorted from small to big as ASCII
                final StringASCSortComparator comparator = new StringASCSortComparator();
                Collections.sort(_recordList, comparator);
                // Get platForm, channelId, loginTime from the firstRecord
                String theFirstRecord = _recordList.get(0);
                _record = theFirstRecord.split("\t", 4);
                if (_record.length == 4) {
                    _loginTime = _record[0];
                    _platForm = _record[1];
                    _channelId = _record[3];

                    // Get productVersion from the lastRecord
                    String theLastRecord = _recordList.get(_recordList.size() - 1);
                    _record = theLastRecord.split("\t", 4);
                    if (_record.length == 4) {
                        _productVersion = _record[2];
                        // convert login time to loginDay
                        _loginDay = ImeiUtils.parseSecondsToYyyyMmDd(_loginTime);
                        // Generate rowKey
                        try {
                            _rowKey = ImeiUtils.generateRowKey(_imei, _product);
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException(e);
                        }
                        // query hbase for the firstDay, calculate intervalDays and got loginCnt
                        Result rs = ImeiHBaseUtils.queryByRowKey(_hTable, CF, _rowKey);
                        if (rs != null) {
                            // exclude the job run several times in one day
                            byte[] lastDateBytes = rs.getValue(Bytes.toBytes(CF), LAST_DATE_BYTES);
                            String OldLastDate = Bytes.toString(lastDateBytes);
                            if (_loginDay.compareTo(OldLastDate) >= 0) {
                                if (_loginDay.compareTo(OldLastDate) > 0) {
                                    // Exist record, has not been processed today, update process.
                                    byte[] firstDateBytes = rs.getValue(Bytes.toBytes(CF), FIRST_DATE_BYTES);
                                    _firstDay = Bytes.toString(firstDateBytes);
                                    byte[] loginCntBytes = rs.getValue(Bytes.toBytes(CF), LOGIN_CNT_BYTES);
                                    _loginCnt = String.valueOf(Integer.parseInt(Bytes.toString(loginCntBytes)) + 1);
                                    try {
                                        _intervalDays = String.valueOf(ImeiUtils.calcIntervalDays(_firstDay, _loginDay) + 1); // interval + 1
                                    } catch (ParseException e) {
                                        throw new RuntimeException(e);
                                    }
                                } else {
                                    // Exist record, has been processed today. Got exist firstDay, intervalDays, loginCnt
                                    byte[] firstDateBytes = rs.getValue(Bytes.toBytes(CF), FIRST_DATE_BYTES);
                                    _firstDay = Bytes.toString(firstDateBytes);
                                    byte[] loginCntBytes = rs.getValue(Bytes.toBytes(CF), LOGIN_CNT_BYTES);
                                    _loginCnt = String.valueOf(Integer.parseInt(Bytes.toString(loginCntBytes)));
                                    try {
                                        _intervalDays = String.valueOf(ImeiUtils.calcIntervalDays(_firstDay, _loginDay) + 1); // interval
                                    } catch (ParseException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        } else {
                            // new record, insert process.
                            _firstDay = _loginDay;
                            _intervalDays = "1";
                            _loginCnt = "1";
                        }

                        // make new Key
                        _keyBuilder.append(_product).append("\t")
                                .append(_platForm).append("\t")
                                .append(_channelId).append("\t")
                                .append(_productVersion).append("\t")
                                .append(_firstDay).append("\t")
                                .append(_loginDay).append("\t")
                                .append(_intervalDays).append("\t")
                                .append(_loginCnt);
                        _newKey.set(_keyBuilder.toString());
                        _keyBuilder.setLength(0);
                        // make new Value
                        _newValue.set(_imei);

                        context.write(_newKey, _newValue);
                    }
                }
            }
            // clean the recordList.
            _recordList.clear();
        }
    }

    public static class ReducerProcess extends Reducer<Text, Text, Text, Text> {
        // key
        // 0. product;
        // 1. platForm;
        // 2. channelId;
        // 3. productVersion;
        // 4. firstDay;
        // 5. loginDay;
        // 6. intervalDays;
        // 7. loginCnt;
        // value

        private List<String> _imeiList = new ArrayList<String>();
        private List<Put> _putList = new ArrayList<Put>();
        // New Key
        private StringBuilder _keyBuilder = new StringBuilder(MAX_NEW_KEY);
        private Text _newKey = new Text();
        private Text _newValue = new Text();
        // HTable
        //private String _rowKey;
        private String _tableName;
        private HTable _hTable;

        @Override
        protected void setup(Context context) throws IOException {
            //初始化hTable处理对象，关闭自动提交，设置写入Buffer为64M
            _tableName = context.getConfiguration().get(AbstractJobStart.TABLE_NAME_PARA);
            _hTable = new HTable(context.getConfiguration(), _tableName);
            _hTable.setAutoFlush(true);
            _hTable.setWriteBufferSize(64 * 1024 * 1024);
            //this.startTime = System.currentTimeMillis();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Get columns form key
            String rowKey;
            String[] record = key.toString().split("\t", 8);
            // whether the record length is 5 or not
            if (record.length == 8) {
                ImeiProductUser imeiProductUser = new ImeiProductUser();
                imeiProductUser.set_product(record[0]);
                imeiProductUser.set_platForm(record[1]);
                imeiProductUser.set_channelId(record[2]);
                imeiProductUser.set_productVersion(record[3]);
                imeiProductUser.set_firstDay(record[4]);
                imeiProductUser.set_lastDay(record[5]);
                imeiProductUser.set_intervalDays(record[6]);
                imeiProductUser.set_loginCnt(record[7]);
                for (Text value : values) {
                    _imeiList.add(value.toString());
                }
                if (imeiProductUser.get_intervalDays().equals("1")) {
                    // New rowKey, insert.
                    for (String imei : _imeiList) {
                        try {
                            rowKey = ImeiUtils.generateRowKey(imei, imeiProductUser.get_product());
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException(e);
                        }
                        Put put = ImeiHBaseUtils.createInsertPut(rowKey, imeiProductUser);
                        if (put != null) {
                            _putList.add(put);
                        }
                    }
                } else {
                    // Exist rowKey, update.
                    for (String imei : _imeiList) {
                        try {
                            rowKey = ImeiUtils.generateRowKey(imei, imeiProductUser.get_product());
                        } catch (NoSuchAlgorithmException e) {
                            throw new RuntimeException(e);
                        }
                        Put put = ImeiHBaseUtils.createUpdatePut(rowKey, imeiProductUser);
                        _putList.add(put);
                    }
                }
                // put to HTable

                _hTable.put(_putList);
                // write to context
                imeiProductUser.set_firstDay(imeiProductUser.get_firstDay() + " 00:00:00");
                imeiProductUser.set_lastDay(imeiProductUser.get_lastDay() + " 00:00:00");
                // make new key
                _keyBuilder.append(imeiProductUser.get_firstDay()).append("\t")
                        .append(imeiProductUser.get_lastDay()).append("\t")
                        .append(imeiProductUser.get_intervalDays()).append("\t")
                        .append(imeiProductUser.get_product()).append("\t")
                        .append(imeiProductUser.get_channelId()).append("\t")
                        .append(imeiProductUser.get_platForm()).append("\t")
                        .append(imeiProductUser.get_productVersion()).append("\t")
                        .append(_imeiList.size());
                _newKey.set(_keyBuilder.toString());
                _keyBuilder.setLength(0);
                _newValue.set("");
                context.write(_newKey, _newValue);
                // clear
                _imeiList.clear();
                _putList.clear();
            }
        }
    }
}