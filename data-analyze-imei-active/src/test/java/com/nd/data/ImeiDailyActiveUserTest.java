package com.nd.data;

import com.nd.mapred.AbstractJobStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created with IntelliJ IDEA. User: Lin QiLi Date: 14-2-19 Time: 上午11:09
 */
@SuppressWarnings("ALL")
public class ImeiDailyActiveUserTest {

    private String[] _mapInputLineArr = {
        //UIN platForm    product productVersion  IMEI    channelId   loginTime
        "494082107\t8\t100111\t1\t356261050374378\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsD\t1383321647",
        "494082107\t8\t100111\t2\t356261050374378\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsE\t1383321648",
        "494082107\t8\t100111\t3\t356261050374378\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsF\t1383321649",
        "351750788\t1\t100111\t1\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\txhF3F2lMKRBs+01vqrRbk+zk416DQc1O\t1383321647",
        "351750788\t1\t100111\t2\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\txhF3F2lMKRBs+01vqrRbk+zk416DQc11\t1383321648",
        "351750788\t1\t100111\t3\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\txhF3F2lMKRBs+01vqrRbk+zk416DQc12\t1383321649",
        "351750788\t1\t100111\t4\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\txhF3F2lMKRBs+01vqrRbk+zk416DQc13\t1383321650",
        "550208653\t8\t102455\t\tA0000038355CC5\t\t1383321647",
        "445935401\t8\t106538\t\t353771057560863\tjZg3GleBqu8L/foOsDxcb2hD4LFsYsvUFU90xSSLfdM=\t1383321647", //            "445935501\t\t110110\t\t\t\t1383321888"
    };
    private String[] _mapOutputLineArr = {
        // product imei loginTime platFrom productVersion channelId
        "100111\t356261050374378\t1383321647\t8\t1\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsD",
        "100111\t356261050374378\t1383321648\t8\t2\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsE",
        "100111\t356261050374378\t1383321649\t8\t3\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsF",
        "100111\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t1383321647\t1\t1\txhF3F2lMKRBs+01vqrRbk+zk416DQc1O",
        "100111\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t1383321648\t1\t2\txhF3F2lMKRBs+01vqrRbk+zk416DQc11",
        "100111\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t1383321649\t1\t3\txhF3F2lMKRBs+01vqrRbk+zk416DQc12",
        "100111\tebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t1383321650\t1\t4\txhF3F2lMKRBs+01vqrRbk+zk416DQc13",
        "102455\tA0000038355CC5\t1383321647\t8\t\t",
        "106538\t353771057560863\t1383321647\t8\t\tjZg3GleBqu8L/foOsDxcb2hD4LFsYsvUFU90xSSLfdM="
    };
    private String[] _combinerOutputLineArr = {
        //imei product platForm channelId productVersion firstDay loginDay intervalDays loginCnt
        "356261050374378\t100111\t8\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsD\t3\t2013-11-02\t2013-11-02\t1\t1",
        //"356261050374378\t100111\t8\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsE\t2\t2013-11-2\t2013-11-2\t1\t1",
        //"356261050374378\t100111\t8\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsF\t3\t2013-11-2\t2013-11-2\t1\t1",
        "ebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t100111\t1\txhF3F2lMKRBs+01vqrRbk+zk416DQc1O\t4\t2013-11-02\t2013-11-02\t1\t1",
        //"ebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t100111\t1\txhF3F2lMKRBs+01vqrRbk+zk416DQc11\t2\t2013-11-2\t2013-11-2\t1\t1",
        //"ebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t100111\t1\txhF3F2lMKRBs+01vqrRbk+zk416DQc12\t3\t2013-11-2\t2013-11-2\t1\t1",
        //"ebd63a92d739a743d05d2dfef71dd94bfb4c1f57\t100111\t1\txhF3F2lMKRBs+01vqrRbk+zk416DQc13\t4\t2013-11-2\t2013-11-2\t1\t1",
        "A0000038355CC5\t102455\t8\t\t\t2013-11-02\t2013-11-02\t1\t1",
        "353771057560863\t106538\t8\tjZg3GleBqu8L/foOsDxcb2hD4LFsYsvUFU90xSSLfdM=\t\t2013-11-02\t2013-11-02\t1\t1"
    };
    private String[] _reducerOutputLineArr = {
        "2013-11-02 00:00:00\t2013-11-02 00:00:00\t1\t100111\tjZg3GleBqu9XdELIGKzVciBgaPmmhCsD\t8\t3\t1",
        "2013-11-02 00:00:00\t2013-11-02 00:00:00\t1\t100111\txhF3F2lMKRBs+01vqrRbk+zk416DQc1O\t1\t4\t1",
        "2013-11-02 00:00:00\t2013-11-02 00:00:00\t1\t102455\t\t8\t\t1",
        "2013-11-02 00:00:00\t2013-11-02 00:00:00\t1\t106538\tjZg3GleBqu8L/foOsDxcb2hD4LFsYsvUFU90xSSLfdM=\t8\t\t1"
    };
    private Mapper _mapper;
    private Reducer _combiner;
    private Reducer _reducer;

    public ImeiDailyActiveUserTest() {
    }

    @BeforeClass
    public static void beforeClass() {
        System.out.println("global...");
    }

    @AfterClass
    public static void afterClass() {
        System.out.println("global destroy...");
    }

    @Before
    public void setUp() {
        _mapper = new ImeiDailyActiveUser.MapperProcess();
        _combiner = new ImeiDailyActiveUser.CombinerProcess();
        _reducer = new ImeiDailyActiveUser.ReducerProcess();
        System.out.println("mapper/reducer setup...");
    }

    @After
    public void tearDown() {
    }

    private void initConfiguration(TestDriver testDriver) {
        Configuration config = HBaseConfiguration.create();
        Iterator<Entry<String, String>> iterator = config.iterator();
        Entry<String, String> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();
            testDriver.getConfiguration().set(entry.getKey(), entry.getValue());
        }
        testDriver.getConfiguration().set(AbstractJobStart.TABLE_NAME_PARA, "Test_DATA_Product_IMEI");
    }

    private Map<String, String> parseMapperInputToOutput(String line) {
        Map<String, String> resultMap = new HashMap<String, String>(2, 1);
        String platForm;           // 1. platForm
        String product;            // 2. productID
        String productVersion;     // 3. product version
        String imei;               // 4. imei
        String channelId;          // 5. channel ID
        String loginTime;          // 6. LoginTime
        StringBuilder keyBuilder = new StringBuilder(400);
        StringBuilder valueBuilder = new StringBuilder(400);

        String[] record = line.split("\t");
        platForm = record[1];
        product = record[2];
        productVersion = record[3];
        imei = record[4];
        channelId = record[5];
        loginTime = record[6];

        // make new key
        keyBuilder.append(product).append("\t").append(imei);
        resultMap.put("key", keyBuilder.toString());
        keyBuilder.setLength(0);
        // make new value
        valueBuilder.append(loginTime).append("\t")
                .append(platForm).append("\t")
                .append(productVersion).append("\t")
                .append(channelId);
        resultMap.put("value", valueBuilder.toString());
        valueBuilder.setLength(0);

        return resultMap;
    }

    @Test
    public void mapperTest() throws IOException {
        MapDriver mapDriver = new MapDriver(_mapper);
        for (String line : _mapInputLineArr) {
            mapDriver.withInput(NullWritable.get(), new Text(line));
        }

        String[] record;
        for (String line : _mapOutputLineArr) {
            record = line.split("\t", 3);
            String product = record[0];
            String imei = record[1];
            String value = record[2];

            StringBuilder keyBuilder = new StringBuilder(400);
            keyBuilder.append(product).append("\t")
                    .append(imei);
            mapDriver.withOutput(new Text(keyBuilder.toString()), new Text(value));
            keyBuilder.setLength(0);
        }
        mapDriver.runTest();
    }

    @Test
    public void combinerTest() throws IOException {
        ReduceDriver reduceDriver = new ReduceDriver(_combiner);
        this.initConfiguration(reduceDriver);
        reduceDriver.getConfiguration();
        String lastKey = "";
        String key;
        String value;
        String[] record;
        List<Text> valueList = new ArrayList<Text>(5);
        for (String line : _mapOutputLineArr) {
            record = line.split("\t", 3);
            String product = record[0];
            String imei = record[1];
            value = record[2];
            key = product + "\t" + imei;
            if (!key.equals(lastKey)) {
                if (!lastKey.isEmpty()) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                }
                lastKey = key;
                valueList = new ArrayList<Text>(5);
            }
            valueList.add(new Text(value));
        }
        if (!lastKey.isEmpty()) {
            reduceDriver.withInput(new Text(lastKey), valueList);
        }
        //
        for (String line : _combinerOutputLineArr) {
            record = line.split("\t", 2);
            reduceDriver.withOutput(new Text(record[1]), new Text(record[0]));
        }
        reduceDriver.runTest();
    }

    @Test
    public void reduceTest() throws IOException {
        ReduceDriver reduceDriver = new ReduceDriver(_reducer);
        this.initConfiguration(reduceDriver);
        String lastKey = "";
        String key;
        String value;
        String[] record;
        List<Text> valueList = new ArrayList<Text>(5);
        for (String line : _combinerOutputLineArr) {
            record = line.split("\t", 2);
            key = record[1];
            value = record[0];
            if (!key.equals(lastKey)) {
                if (!lastKey.isEmpty()) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                }
                lastKey = key;
                valueList = new ArrayList<Text>(5);
            }
            valueList.add(new Text(value));
        }
        if (!lastKey.isEmpty()) {
            reduceDriver.withInput(new Text(lastKey), valueList);
        }

        for (String line : _reducerOutputLineArr) {
            reduceDriver.withOutput(new Text(line), new Text(""));
        }
        //
        reduceDriver.runTest();
    }
}