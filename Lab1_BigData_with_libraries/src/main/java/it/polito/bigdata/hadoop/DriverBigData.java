package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DriverBigData extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path inputPath;
        Path outputDir;
        int numberOfReducers;
        int exitCode;

        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]);

        Job job = getJob(inputPath, outputDir, numberOfReducers);

        if (job.waitForCompletion(true))
            exitCode = 0;
        else
            exitCode = 1;

        return exitCode;
    }

    private Job getJob(Path inputPath, Path outputDir, int numberOfReducers) throws IOException {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Basic MapReduce Project - WordCount example");
        //if it is a folder, the job reads all the
        // files in the specified folder) for this job
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setJarByClass(DriverBigData.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapperBigData.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(ReducerBigData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(numberOfReducers);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

        System.exit(res);
    }
}