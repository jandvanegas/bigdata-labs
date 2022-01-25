package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.MessageFormat;


public class DriverBigData extends Configured implements Tool {

    public static final String FILTERING_WORD_KEY = "FILTERING_WORD_KEY";

    @Override
    public int run(String[] args) throws Exception {


        int exitCode = 0;

        // Change the following part of the code

        Path inputPath;
        Path outputDir;
        int numberOfReducers;
        String filterWord;


        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]);
        filterWord = args[3];

        Configuration conf = this.getConf();
        conf.set(FILTERING_WORD_KEY, filterWord);

        // Define a new job
        Job job = Job.getInstance(conf);
        job.setJobName("Lab2 - Skeleton");

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

        if (!job.waitForCompletion(true)) {
            exitCode = 1;
        }
        Counter matches = job.getCounters().findCounter(COUNTERS.FILTER_MATCHES);
        Counter noMatches = job.getCounters().findCounter(COUNTERS.FILTER_NO_MATCHES);
        System.out.println(MessageFormat.format("Matches {0} No matches {1}", matches.getValue(), noMatches.getValue()));

        return exitCode;

    }


    public static enum COUNTERS {
        FILTER_MATCHES,
        FILTER_NO_MATCHES
    }

    public static void main(String args[]) throws Exception {
        // Exploit the ToolRunner class to "configure" and run the Hadoop application
        int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

        System.exit(res);
    }
}

