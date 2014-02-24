package com.nd.data.uin;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static com.nd.data.util.HbaseTableUtil.*;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author aladdin
 */
public class UinChangeMapred {

    public static class MyMapper extends TableMapper<Text, Text> {

        //map输出
        private final Text newKey = new Text();
        private final Text newValue = new Text();
        private final StringBuilder keyBuilder = new StringBuilder(256);
        private final String[] fields = {
            "uin", "firstDate", "lastDate", "product", "channelId", "platForm",
            "productVersion", "loginCnt", "leave01Date", "leave07Date", "leave14Date",
            "leave30Date", "leave01Cnt", "leave07Cnt", "leave14Cnt", "leave30Cnt"
        };

        /**
         * 读取hbase产品用户总表内容，输出变化记录
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
                //构造out key，列之前用\t间隔
                //uin,firstDate,lastDate,product,channelId,platform,productVersion,loginCnt,
                //leave01Date,leave01Cnt,leave07Date,leave07Cnt,leave14Date,leave14Cnt,leave30Date,leave30Cnt
                if (flag) {
                    //有效记录，输出
                    String uin = fieldValueMap.get("uin");
                    String firstDate = fieldValueMap.get("firstDate");
                    String lastDate = fieldValueMap.get("lastDate");
                    String product = fieldValueMap.get("product");
                    String channelId = fieldValueMap.get("channelId");
                    String platForm = fieldValueMap.get("platForm");
                    String productVersion = fieldValueMap.get("productVersion");
                    String loginCnt = fieldValueMap.get("loginCnt");
                    String leave01Date = fieldValueMap.get("leave01Date");
                    String leave07Date = fieldValueMap.get("leave07Date");
                    String leave14Date = fieldValueMap.get("leave14Date");
                    String leave30Date = fieldValueMap.get("leave30Date");
                    String leave01Cnt = fieldValueMap.get("leave01Cnt");
                    String leave07Cnt = fieldValueMap.get("leave07Cnt");
                    String leave14Cnt = fieldValueMap.get("leave14Cnt");
                    String leave30Cnt = fieldValueMap.get("leave30Cnt");
                    this.keyBuilder.append(uin).append('\t').append(firstDate).append('\t')
                            .append(lastDate).append('\t').append(product).append('\t')
                            .append(channelId).append('\t').append(platForm).append('\t')
                            .append(productVersion).append('\t').append(loginCnt).append('\t')
                            .append(leave01Date).append('\t').append(leave01Cnt).append('\t')
                            .append(leave07Date).append('\t').append(leave07Cnt).append('\t')
                            .append(leave14Date).append('\t').append(leave14Cnt).append('\t')
                            .append(leave30Date).append('\t').append(leave30Cnt);
                    this.newKey.set(this.keyBuilder.toString());
                    this.keyBuilder.setLength(0);
                    //输出
                    context.write(this.newKey, this.newValue);
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        private final Text newValue = new Text();

        /**
         * 获取mapper的输出信息,输出hdfs
         *
         * @param key
         * @param values
         * @param context
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, this.newValue);
        }
    }
}
