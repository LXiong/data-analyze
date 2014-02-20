package com.nd.data.imei.lifecycle.times;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static com.nd.data.imei.lifecycle.times.ImeiLifecycleTimesMapred.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

/**
 *
 * @author aladdin
 */
public class ImeiLifecycleTimesMapredJUnitTest {

    public ImeiLifecycleTimesMapredJUnitTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * 初始化测试驱动的hadoop上下文信息
     *
     * @param testDriver
     */
    private void initHadoopConfiguration(TestDriver testDriver) {
        Configuration config = HBaseConfiguration.create();
        Iterator<Entry<String, String>> iterator = config.iterator();
        Entry<String, String> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();
            testDriver.getConfiguration().set(entry.getKey(), entry.getValue());
        }
    }
    private final Mapper mapper = new ImeiLifecycleTimesMapred.MyMapper();
    private final Reducer reducer = new ImeiLifecycleTimesMapred.MyReducer();
    private final String stateDate = "2014-01-17";
    private final String tableName = "Test_DATA_Product_IMEI";
    private final String[] mapperInputData = {
        "1013	2014-01-01	2014-01-17	1	1	1	1	13",
        "1014	2014-01-01	2014-01-17	1	1	1	1	14",
        "1015	2014-01-01	2014-01-17	1	1	1	1	15",
        "1016	2014-01-01	2014-01-17	1	1	1	1	16",
        "1017	2014-01-01	2014-01-17	1	1	1	1	17",
    };
    private final String[] mapperOutputData = {
        "1013	2014-01-01	13	leave01	1	1	1	1",
        "1013	2014-01-01	13	leave07	1	1	1	1",
        "1013	2014-01-01	13	leave14	1	1	1	1",
        "1013	2014-01-01	13	leave30	1	1	1	1",
        "1014	2014-01-01	14	leave01	1	1	1	1",
        "1014	2014-01-01	14	leave07	1	1	1	1",
        "1014	2014-01-01	14	leave14	1	1	1	1",
        "1014	2014-01-01	14	leave30	1	1	1	1",
        "1015	2014-01-01	15	leave01	1	1	1	1",
        "1015	2014-01-01	15	leave07	1	1	1	1",
        "1015	2014-01-01	15	leave14	1	1	1	1",
        "1015	2014-01-01	15	leave30	1	1	1	1",
        "1016	2014-01-01	16	leave01	1	1	1	1",
        "1016	2014-01-01	16	leave07	1	1	1	1",
        "1016	2014-01-01	16	leave14	1	1	1	1",
        "1016	2014-01-01	16	leave30	1	1	1	1",
        "1017	2014-01-01	17	leave01	1	1	1	1",
        "1017	2014-01-01	17	leave07	1	1	1	1",
        "1017	2014-01-01	17	leave14	1	1	1	1",
        "1017	2014-01-01	17	leave30	1	1	1	1",
    };

    @Test
    public void mapperTest() throws IOException {
        MapDriver mapDriver = new MapDriver(this.mapper);
        //构造hbase输入
        long startKey = System.currentTimeMillis();
        ImmutableBytesWritable key;
        Result result;
        String[] fields;
        List<KeyValue> list;
        KeyValue keyValue;
        byte[] rowKey;
        for (int index = 0; index < this.mapperInputData.length; index++) {
            rowKey = Bytes.toBytes(Long.toString(startKey));
            key = new ImmutableBytesWritable(rowKey);
            fields = this.mapperInputData[index].split("\t");
            list = new ArrayList<KeyValue>();
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, IMEI, Bytes.toBytes(fields[0]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, FIRST_DATE, Bytes.toBytes(fields[1]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LAST_DATE, Bytes.toBytes(fields[2]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, PRODUCT, Bytes.toBytes(fields[3]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, CHANNEL_ID, Bytes.toBytes(fields[4]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, PLAT_FORM, Bytes.toBytes(fields[5]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, PRODUCT_VERSION, Bytes.toBytes(fields[6]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LOGIN_CNT, Bytes.toBytes(fields[7]));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE01_CNT, Bytes.toBytes(""));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE07_CNT, Bytes.toBytes(""));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE14_CNT, Bytes.toBytes(""));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE30_CNT, Bytes.toBytes(""));
            list.add(keyValue);
            //对keyValu进行排序，如果没有排序，再调用result.getValue()方法时将无法正确查找结果,祥见Result.binarySearch()
            Collections.sort(list, KeyValue.COMPARATOR);
            result = new Result(list.toArray(new KeyValue[list.size()]));
            mapDriver.withInput(key, result);
            startKey++;

        }
        //构造mapper输出
        for (int index = 0; index < this.mapperOutputData.length; index++) {
            fields = this.mapperOutputData[index].split("\t", 2);
            mapDriver.withOutput(new Text(fields[1]), new Text(fields[0]));
        }
        //设置环境变量
        this.initHadoopConfiguration(mapDriver);
        mapDriver.getConfiguration().set(STATE_DATE_NAME, this.stateDate);
        mapDriver.getConfiguration().set(TABLE_NAME, this.tableName);
        //测试
        mapDriver.runTest();
    }
    private final String[] reducerInputData = {
        "1013	2014-01-01	13	leave01	1	1	1	1",
        "1014	2014-01-01	14	leave01	1	1	1	1",
        "1015	2014-01-01	15	leave01	1	1	1	1",
        "1016	2014-01-01	16	leave01	1	1	1	1",
        "1017	2014-01-01	17	leave01	1	1	1	1",
        "1013	2014-01-01	13	leave07	1	1	1	1",
        "1014	2014-01-01	14	leave07	1	1	1	1",
        "1015	2014-01-01	15	leave07	1	1	1	1",
        "1016	2014-01-01	16	leave07	1	1	1	1",
        "1017	2014-01-01	17	leave07	1	1	1	1",
        "1013	2014-01-01	13	leave14	1	1	1	1",
        "1014	2014-01-01	14	leave14	1	1	1	1",
        "1015	2014-01-01	15	leave14	1	1	1	1",
        "1016	2014-01-01	16	leave14	1	1	1	1",
        "1017	2014-01-01	17	leave14	1	1	1	1",
        "1013	2014-01-01	13	leave30	1	1	1	1",
        "1014	2014-01-01	14	leave30	1	1	1	1",
        "1015	2014-01-01	15	leave30	1	1	1	1",
        "1016	2014-01-01	16	leave30	1	1	1	1",
        "1017	2014-01-01	17	leave30	1	1	1	1"
    };
    private final String[] reducerOutputDate = {
        "2014-01-01	13	leave01	1	1	1	1	1",
        "2014-01-01	14	leave01	1	1	1	1	1",
        "2014-01-01	15	leave01	1	1	1	1	1",
        "2014-01-01	16	leave01	1	1	1	1	1",
        "2014-01-01	17	leave01	1	1	1	1	1",
        "2014-01-01	13	leave07	1	1	1	1	1",
        "2014-01-01	14	leave07	1	1	1	1	1",
        "2014-01-01	15	leave07	1	1	1	1	1",
        "2014-01-01	16	leave07	1	1	1	1	1",
        "2014-01-01	17	leave07	1	1	1	1	1",
        "2014-01-01	13	leave14	1	1	1	1	1",
        "2014-01-01	14	leave14	1	1	1	1	1",
        "2014-01-01	15	leave14	1	1	1	1	1",
        "2014-01-01	16	leave14	1	1	1	1	1",
        "2014-01-01	17	leave14	1	1	1	1	1",
        "2014-01-01	13	leave30	1	1	1	1	1",
        "2014-01-01	14	leave30	1	1	1	1	1",
        "2014-01-01	15	leave30	1	1	1	1	1",
        "2014-01-01	16	leave30	1	1	1	1	1",
        "2014-01-01	17	leave30	1	1	1	1	1"
    };

    @Test
    public void reducerTest() throws IOException {
        ReduceDriver reduceDriver = new ReduceDriver(this.reducer);
        //构造reducer输入
        String[] fields;
        String lastKey = "";
        String key;
        String value;
        List<Text> valueList = new ArrayList<Text>();
        for (int index = 0; index < this.reducerInputData.length; index++) {
            fields = this.reducerInputData[index].split("\t", 2);
            key = fields[1];
            value = fields[0];
            if (key.equals(lastKey) == false) {
                if (lastKey.isEmpty() == false) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                }
                lastKey = key;
                valueList = new ArrayList<Text>();
            }
            valueList.add(new Text(value));
        }
        if (lastKey.isEmpty() == false) {
            reduceDriver.withInput(new Text(lastKey), valueList);
        }
        //构造reducer输出

        for (int index = 0; index < this.reducerOutputDate.length; index++) {
            reduceDriver.withOutput(new Text(this.reducerOutputDate[index]), new Text());
        }
        //测试
        reduceDriver.runTest();
    }
}