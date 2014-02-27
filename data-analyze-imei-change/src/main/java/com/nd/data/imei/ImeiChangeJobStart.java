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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author aladdin
 */
public class ImeiChangeJobStart extends AbstractJobStart {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new ImeiChangeJobStart(), args);
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
        final Job job = new Job(conf, "data-analyze-imei-change");
        job.setJarByClass(ImeiChangeMapred.class);
        job.setMapSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);
        job.setNumReduceTasks(16);
        //初始化kerbros
        TableMapReduceUtil.initCredentials(job);
        //设置hbase输入
        final Scan scan = new Scan();
        scan.setMaxVersions();
        scan.setCacheBlocks(false);
        scan.setCaching(5000);
        //过滤lastUpdateDate >= stateDate的记录
        Filter filter = new SingleColumnValueFilter(COLUMN_FAMILY, LAST_UPDATE_DATE, CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(stateDate));
        scan.setFilter(filter);
        TableMapReduceUtil.initTableMapperJob(
                inputTableName,
                scan,
                ImeiChangeMapred.MyMapper.class,
                Text.class,
                Text.class,
                job);
        //设置hdfs输出
        job.setReducerClass(ImeiChangeMapred.MyReducer.class);
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
