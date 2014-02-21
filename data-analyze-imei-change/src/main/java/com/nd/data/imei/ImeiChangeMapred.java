package com.nd.data.imei;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static com.nd.data.util.HbaseTableUtil.*;

/**
 *
 * @author aladdin
 */
public class ImeiChangeMapred {

    public static class MyMapper extends TableMapper<Text, Text> {

        //map输出
        private final Text newKey = new Text();
        private final Text newValue = new Text();
        private final StringBuilder keyBuilder = new StringBuilder(256);

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
                final String imei = Bytes.toString(value.getValue(COLUMN_FAMILY, IMEI));
                final String firstDate = Bytes.toString(value.getValue(COLUMN_FAMILY, FIRST_DATE));
                final String lastDate = Bytes.toString(value.getValue(COLUMN_FAMILY, LAST_DATE));
                final String product = Bytes.toString(value.getValue(COLUMN_FAMILY, PRODUCT));
                final String channelId = Bytes.toString(value.getValue(COLUMN_FAMILY, CHANNEL_ID));
                final String platForm = Bytes.toString(value.getValue(COLUMN_FAMILY, PLAT_FORM));
                final String productVersion = Bytes.toString(value.getValue(COLUMN_FAMILY, PRODUCT_VERSION));
                final String loginCnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LOGIN_CNT));
                final String leave01Date = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE01_DATE));
                final String leave07Date = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE07_DATE));
                final String leave14Date = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE14_DATE));
                final String leave30Date = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE30_DATE));
                final String leave01Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE01_CNT));
                final String leave07Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE07_CNT));
                final String leave14Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE14_CNT));
                final String leave30Cnt = Bytes.toString(value.getValue(COLUMN_FAMILY, LEAVE30_CNT));
                //构造out key，列之前用\t间隔
                //imei,firstDate,lastDate,product,channelId,platform,productVersion,loginCnt,
                //leave01Date,leave01Cnt,leave07Date,leave07Cnt,leave14Date,leave14Cnt,leave30Date,leave30Cnt
                this.keyBuilder.append(imei).append('\t').append(firstDate).append('\t')
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
