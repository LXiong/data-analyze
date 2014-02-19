package com.nd.data.uin.lifecycle.interval;

import com.nd.mapred.AbstractJobStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import static com.nd.data.uin.lifecycle.interval.UinLifecycleIntervalMapred.*;

/**
 *
 * @author aladdin
 */
public class JobStart extends AbstractJobStart {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new JobStart(), args);
        System.exit(res);
    }

    @Override
    public Job createJob() throws Exception {
        Configuration conf = this.getConf();
        //获取job输入参数
        final String inputTableName = this.getParameter("inputTableName");
        final String stateDate = this.getParameter("stateDate");
        final String outputPath = this.getParameter("outputPath");
        //初始化job
        final Job job = new Job(conf, "data-analyze-uin-lifecycle-interval");
        //设置hbase输入
        final Scan scan = new Scan();
        scan.setMaxVersions();
        scan.setCacheBlocks(false);
        scan.addColumn(COLUMN_FAMILY, UIN);
        scan.addColumn(COLUMN_FAMILY, FIRST_DATE);
        scan.addColumn(COLUMN_FAMILY, LAST_DATE);
        scan.addColumn(COLUMN_FAMILY, PRODUCT);
        scan.addColumn(COLUMN_FAMILY, CHANNEL_ID);
        scan.addColumn(COLUMN_FAMILY, PLAT_FORM);
        scan.addColumn(COLUMN_FAMILY, PRODUCT_VERSION);
        scan.addColumn(COLUMN_FAMILY, LEAVE01_DATE);
        scan.addColumn(COLUMN_FAMILY, LEAVE07_DATE);
        scan.addColumn(COLUMN_FAMILY, LEAVE14_DATE);
        scan.addColumn(COLUMN_FAMILY, LEAVE30_DATE);
        //获取30日未失效的用户记录
        final Filter filter = new SingleColumnValueFilter(COLUMN_FAMILY, LEAVE30_DATE, CompareOp.EQUAL, Bytes.toBytes(""));
        scan.setFilter(filter);
        scan.setCaching(500);
        TableMapReduceUtil.initTableMapperJob(
                inputTableName,
                scan,
                MyMapper.class,
                Text.class,
                Text.class,
                job);
        //设置环境变量
        job.getConfiguration().set(STATE_DATE_NAME, stateDate);
        job.getConfiguration().set(TABLE_NAME, inputTableName);
        //设置hdfs输出
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //初始化kerbros
        TableMapReduceUtil.initCredentials(job);
        return job;
    }

    @Override
    public String[] getValidateParameter() {
        String[] paras = {"inputTableName", "stateDate", "outputPath"};
        return paras;
    }
}
