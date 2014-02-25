package com.nd.data;

import com.nd.mapred.AbstractJobStart;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA. User: Lin QiLi Date: 14-2-20 Time: 下午7:28
 */
public class UinDailyActiveUserStart extends AbstractJobStart {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new UinDailyActiveUserStart(), args);
        System.exit(res);
    }

    @Override
    public Job createJob() throws IOException {
        Configuration conf = this.getConf();

        final String tableName = this.getParameter("tableName");
        final String inputPath = this.getParameter("inputPath");
        final String outputPath = this.getParameter("outputPath");
        final String numReducerTask = this.getParameter("numReducerTask");

        Job job = new Job(conf, "data-analyze-uin-active");
        job.setJarByClass(UinDailyActiveUser.class);
        job.setMapperClass(UinDailyActiveUser.MapperProcess.class);
        job.setCombinerClass(UinDailyActiveUser.CombinerProcess.class);
        job.setReducerClass(UinDailyActiveUser.ReducerProcess.class);
        //job.setPartitionerClass(AbstractJobStart.HTablePartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(Integer.parseInt(numReducerTask));

        // Input and Output
        String[] inputPaths = inputPath.split(",");
        for (String path : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(path));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.getConfiguration().set(AbstractJobStart.TABLE_NAME_PARA, tableName);

        //final int loadFactory = Integer.parseInt(this.getParameter("loadFactor"));
        //final int distance = Integer.parseInt(this.getParameter("distance"));
        //this.initJobByHTablePartition(job, tableName, loadFactory, distance);
        TableMapReduceUtil.initCredentials(job);

        return job;
    }

    @Override
    public String[] getValidateParameter() {
        return new String[]{"tableName", "inputPath", "loadFactor", "distance", "outputPath", "numReducerTask"};
    }
}