package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DriverBigData extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        int exitCode = 1;
        Configuration conf = this.getConf();
        Path inputPath = new Path(args[1]);
        Path outputDir = new Path(args[2]);
        int numberOfReducersJob1 = Integer.parseInt(args[0]);
        Job job = getFirstStepJob(conf, inputPath, outputDir, numberOfReducersJob1);
        if (job.waitForCompletion(true)) {
            Path outputDir2 = new Path(args[3]);
            int numberOfReducersJob2 = 1;
            Job job2 = getSecondStepJob(conf, outputDir, outputDir2, numberOfReducersJob2);
            if (job2.waitForCompletion(true)) {
                exitCode = 0;
            }
        }
        return exitCode;
    }

    private Job getSecondStepJob(Configuration conf, Path outputDir, Path outputDir2, int numberOfReducersJob2) throws IOException {
        Job job2 = Job.getInstance(conf);
        job2.setJobName("Lab#3 - Ex.1 - step 2");
        FileInputFormat.addInputPath(job2, outputDir);
        FileOutputFormat.setOutputPath(job2, outputDir2);
        job2.setJarByClass(DriverBigData.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapperClass(MapperBigData2.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(WordCountWritable.class);
        job2.setReducerClass(ReducerBigData2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(numberOfReducersJob2);
        return job2;
    }

    private Job getFirstStepJob(Configuration conf, Path inputPath, Path outputDir, int numberOfReducersJob1) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJobName("Lab#3 - Ex.1 - step 1");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setJarByClass(DriverBigData.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MapperBigData1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ReducerBigData1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(numberOfReducersJob1);
        return job;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
        System.exit(res);
    }
}
