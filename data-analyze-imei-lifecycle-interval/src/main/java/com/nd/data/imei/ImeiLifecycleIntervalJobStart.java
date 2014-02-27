package com.nd.data.imei;

import com.nd.mapred.AbstractJobStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import static com.nd.data.util.HbaseTableUtil.*;

/**
 *
 * @author aladdin
 */
public class ImeiLifecycleIntervalJobStart extends AbstractJobStart {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new ImeiLifecycleIntervalJobStart(), args);
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
        final Job job = new Job(conf, "data-analyze-imei-lifecycle-interval");
        job.setJarByClass(ImeiLifecycleIntervalMapred.class);
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        job.setNumReduceTasks(16);
        //初始化kerbros
        TableMapReduceUtil.initCredentials(job);
        //设置hbase输入
        final Scan scan = new Scan();
        scan.setMaxVersions();
        scan.setCacheBlocks(false);
        scan.addColumn(COLUMN_FAMILY, IMEI);
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
        scan.setCaching(500);
        TableMapReduceUtil.initTableMapperJob(
                inputTableName,
                scan,
                ImeiLifecycleIntervalMapred.MyMapper.class,
                Text.class,
                Text.class,
                job);
        //设置环境变量
        job.getConfiguration().set(ImeiLifecycleIntervalMapred.STATE_DATE_NAME, stateDate);
        job.getConfiguration().set(ImeiLifecycleIntervalMapred.TABLE_NAME, inputTableName);
        //设置hdfs输出
        job.setReducerClass(ImeiLifecycleIntervalMapred.MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    @Override
    public String[] getValidateParameter() {
        String[] paras = {"inputTableName", "stateDate", "outputPath"};
        return paras;
    }
}
