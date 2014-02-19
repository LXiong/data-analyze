package com.nd.data.imei.lifecycle.interval;

import com.nd.data.imei.lifecycle.interval.ImeiLifecycleIntervalMapred;
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
import static com.nd.data.imei.lifecycle.interval.ImeiLifecycleIntervalMapred.*;
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
public class ImeiLifecycleIntervalMapredJUnitTest {

    public ImeiLifecycleIntervalMapredJUnitTest() {
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
    private final Mapper mapper = new ImeiLifecycleIntervalMapred.MyMapper();
    private final Reducer reducer = new ImeiLifecycleIntervalMapred.MyReducer();
    private final String stateDate = "2014-01-17";
    private final String tableName = "Test_DATA_Product_IMEI";
    private final String[] mapperInputData = {
        "1001	2014-01-01	2014-01-01	1	1	1	1	1",
        "1002	2014-01-01	2014-01-02	1	1	1	1	2",
        "1003	2014-01-01	2014-01-03	1	1	1	1	3",
        "1004	2014-01-01	2014-01-04	1	1	1	1	4",
        "1005	2014-01-01	2014-01-05	1	1	1	1	5",
        "1006	2014-01-01	2014-01-06	1	1	1	1	6",
        "1007	2014-01-01	2014-01-07	1	1	1	1	7",
        "1008	2014-01-01	2014-01-08	1	1	1	1	8",
        "1009	2014-01-01	2014-01-09	1	1	1	1	9",
        "1010	2014-01-01	2014-01-10	1	1	1	1	10",
        "1011	2014-01-01	2014-01-11	1	1	1	1	11",
        "1012	2014-01-01	2014-01-12	1	1	1	1	12",
        "1013	2014-01-01	2014-01-13	1	1	1	1	13",
        "1014	2014-01-01	2014-01-14	1	1	1	1	14",
        "1015	2014-01-01	2014-01-15	1	1	1	1	15",
        "1016	2014-01-01	2014-01-16	1	1	1	1	16",
        "1017	2014-01-01	2014-01-17	1	1	1	1	17",
        "1001	2014-01-01	2014-01-01	2	1	1	1	1",
        "1002	2014-01-01	2014-01-02	2	1	1	1	2",
        "1003	2014-01-01	2014-01-03	2	1	1	1	3",
        "1004	2014-01-01	2014-01-04	2	1	1	1	4",
        "1005	2014-01-01	2014-01-05	2	1	1	1	5",
        "1006	2014-01-01	2014-01-06	2	1	1	1	6",
        "1007	2014-01-01	2014-01-07	2	1	1	1	7",
        "1008	2014-01-01	2014-01-08	2	1	1	1	8",
        "1009	2014-01-01	2014-01-09	2	1	1	1	9",
        "1010	2014-01-01	2014-01-10	2	1	1	1	10",
        "1011	2014-01-01	2014-01-11	2	1	1	1	11",
        "1012	2014-01-01	2014-01-12	2	1	1	1	12",
        "1013	2014-01-01	2014-01-13	2	1	1	1	13",
        "1014	2014-01-01	2014-01-14	2	1	1	1	14",
        "1015	2014-01-01	2014-01-15	2	1	1	1	15",
        "1016	2014-01-01	2014-01-16	2	1	1	1	16",
        "1017	2014-01-01	2014-01-17	2	1	1	1	17"
    };
    private final String[] mapperOutputData = {
        "1001	17	leave30	1	1	1	1",
        "1002	17	leave30	1	1	1	1",
        "1003	17	leave30	1	1	1	1",
        "1004	17	leave14	1	1	1	1",
        "1004	17	leave30	1	1	1	1",
        "1005	17	leave14	1	1	1	1",
        "1005	17	leave30	1	1	1	1",
        "1006	17	leave14	1	1	1	1",
        "1006	17	leave30	1	1	1	1",
        "1007	17	leave14	1	1	1	1",
        "1007	17	leave30	1	1	1	1",
        "1008	17	leave14	1	1	1	1",
        "1008	17	leave30	1	1	1	1",
        "1009	17	leave14	1	1	1	1",
        "1009	17	leave30	1	1	1	1",
        "1010	17	leave14	1	1	1	1",
        "1010	17	leave30	1	1	1	1",
        "1011	17	leave07	1	1	1	1",
        "1011	17	leave14	1	1	1	1",
        "1011	17	leave30	1	1	1	1",
        "1012	17	leave07	1	1	1	1",
        "1012	17	leave14	1	1	1	1",
        "1012	17	leave30	1	1	1	1",
        "1013	17	leave07	1	1	1	1",
        "1013	17	leave14	1	1	1	1",
        "1013	17	leave30	1	1	1	1",
        "1014	17	leave07	1	1	1	1",
        "1014	17	leave14	1	1	1	1",
        "1014	17	leave30	1	1	1	1",
        "1015	17	leave07	1	1	1	1",
        "1015	17	leave14	1	1	1	1",
        "1015	17	leave30	1	1	1	1",
        "1016	17	leave07	1	1	1	1",
        "1016	17	leave14	1	1	1	1",
        "1016	17	leave30	1	1	1	1",
        "1017	17	leave01	1	1	1	1",
        "1017	17	leave07	1	1	1	1",
        "1017	17	leave14	1	1	1	1",
        "1017	17	leave30	1	1	1	1",
        "1001	17	leave30	2	1	1	1",
        "1002	17	leave30	2	1	1	1",
        "1003	17	leave30	2	1	1	1",
        "1004	17	leave14	2	1	1	1",
        "1004	17	leave30	2	1	1	1",
        "1005	17	leave14	2	1	1	1",
        "1005	17	leave30	2	1	1	1",
        "1006	17	leave14	2	1	1	1",
        "1006	17	leave30	2	1	1	1",
        "1007	17	leave14	2	1	1	1",
        "1007	17	leave30	2	1	1	1",
        "1008	17	leave14	2	1	1	1",
        "1008	17	leave30	2	1	1	1",
        "1009	17	leave14	2	1	1	1",
        "1009	17	leave30	2	1	1	1",
        "1010	17	leave14	2	1	1	1",
        "1010	17	leave30	2	1	1	1",
        "1011	17	leave07	2	1	1	1",
        "1011	17	leave14	2	1	1	1",
        "1011	17	leave30	2	1	1	1",
        "1012	17	leave07	2	1	1	1",
        "1012	17	leave14	2	1	1	1",
        "1012	17	leave30	2	1	1	1",
        "1013	17	leave07	2	1	1	1",
        "1013	17	leave14	2	1	1	1",
        "1013	17	leave30	2	1	1	1",
        "1014	17	leave07	2	1	1	1",
        "1014	17	leave14	2	1	1	1",
        "1014	17	leave30	2	1	1	1",
        "1015	17	leave07	2	1	1	1",
        "1015	17	leave14	2	1	1	1",
        "1015	17	leave30	2	1	1	1",
        "1016	17	leave07	2	1	1	1",
        "1016	17	leave14	2	1	1	1",
        "1016	17	leave30	2	1	1	1",
        "1017	17	leave01	2	1	1	1",
        "1017	17	leave07	2	1	1	1",
        "1017	17	leave14	2	1	1	1",
        "1017	17	leave30	2	1	1	1"
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
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE01_DATE, Bytes.toBytes(""));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE07_DATE, Bytes.toBytes(""));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE14_DATE, Bytes.toBytes(""));
            list.add(keyValue);
            keyValue = new KeyValue(rowKey, COLUMN_FAMILY, LEAVE30_DATE, Bytes.toBytes(""));
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
        "1001	17	leave30	1	1	1	1",
        "1002	17	leave30	1	1	1	1",
        "1003	17	leave30	1	1	1	1",
        "1004	17	leave30	1	1	1	1",
        "1005	17	leave30	1	1	1	1",
        "1006	17	leave30	1	1	1	1",
        "1007	17	leave30	1	1	1	1",
        "1008	17	leave30	1	1	1	1",
        "1009	17	leave30	1	1	1	1",
        "1010	17	leave30	1	1	1	1",
        "1011	17	leave30	1	1	1	1",
        "1012	17	leave30	1	1	1	1",
        "1013	17	leave30	1	1	1	1",
        "1014	17	leave30	1	1	1	1",
        "1015	17	leave30	1	1	1	1",
        "1016	17	leave30	1	1	1	1",
        "1017	17	leave30	1	1	1	1",
        "1015	17	leave14	1	1	1	1",
        "1004	17	leave14	1	1	1	1",
        "1005	17	leave14	1	1	1	1",
        "1006	17	leave14	1	1	1	1",
        "1007	17	leave14	1	1	1	1",
        "1008	17	leave14	1	1	1	1",
        "1009	17	leave14	1	1	1	1",
        "1010	17	leave14	1	1	1	1",
        "1011	17	leave14	1	1	1	1",
        "1012	17	leave14	1	1	1	1",
        "1013	17	leave14	1	1	1	1",
        "1014	17	leave14	1	1	1	1",
        "1016	17	leave14	1	1	1	1",
        "1017	17	leave14	1	1	1	1",
        "1011	17	leave07	1	1	1	1",
        "1012	17	leave07	1	1	1	1",
        "1013	17	leave07	1	1	1	1",
        "1014	17	leave07	1	1	1	1",
        "1015	17	leave07	1	1	1	1",
        "1016	17	leave07	1	1	1	1",
        "1017	17	leave07	1	1	1	1",
        "1017	17	leave01	1	1	1	1",
        "1001	17	leave30	2	1	1	1",
        "1002	17	leave30	2	1	1	1",
        "1003	17	leave30	2	1	1	1",
        "1004	17	leave30	2	1	1	1",
        "1005	17	leave30	2	1	1	1",
        "1006	17	leave30	2	1	1	1",
        "1007	17	leave30	2	1	1	1",
        "1008	17	leave30	2	1	1	1",
        "1009	17	leave30	2	1	1	1",
        "1010	17	leave30	2	1	1	1",
        "1011	17	leave30	2	1	1	1",
        "1012	17	leave30	2	1	1	1",
        "1013	17	leave30	2	1	1	1",
        "1014	17	leave30	2	1	1	1",
        "1015	17	leave30	2	1	1	1",
        "1016	17	leave30	2	1	1	1",
        "1017	17	leave30	2	1	1	1",
        "1015	17	leave14	2	1	1	1",
        "1004	17	leave14	2	1	1	1",
        "1005	17	leave14	2	1	1	1",
        "1006	17	leave14	2	1	1	1",
        "1007	17	leave14	2	1	1	1",
        "1008	17	leave14	2	1	1	1",
        "1009	17	leave14	2	1	1	1",
        "1010	17	leave14	2	1	1	1",
        "1011	17	leave14	2	1	1	1",
        "1012	17	leave14	2	1	1	1",
        "1013	17	leave14	2	1	1	1",
        "1014	17	leave14	2	1	1	1",
        "1016	17	leave14	2	1	1	1",
        "1017	17	leave14	2	1	1	1",
        "1011	17	leave07	2	1	1	1",
        "1012	17	leave07	2	1	1	1",
        "1013	17	leave07	2	1	1	1",
        "1014	17	leave07	2	1	1	1",
        "1015	17	leave07	2	1	1	1",
        "1016	17	leave07	2	1	1	1",
        "1017	17	leave07	2	1	1	1",
        "1017	17	leave01	2	1	1	1"
    };
    private final String[] reducerOutputDate = {
        "17	leave30	1	1	1	1	17",
        "17	leave14	1	1	1	1	14",
        "17	leave07	1	1	1	1	7",
        "17	leave01	1	1	1	1	1",
        "17	leave30	2	1	1	1	17",
        "17	leave14	2	1	1	1	14",
        "17	leave07	2	1	1	1	7",
        "17	leave01	2	1	1	1	1"
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
            key = this.stateDate + "\t" + this.reducerOutputDate[index];
            reduceDriver.withOutput(new Text(key), new Text());
        }
        //设置环境环境变量
        this.initHadoopConfiguration(reduceDriver);
        reduceDriver.getConfiguration().set(STATE_DATE_NAME, this.stateDate);
        reduceDriver.getConfiguration().set(TABLE_NAME, this.tableName);
        //测试
        reduceDriver.runTest();
    }
}