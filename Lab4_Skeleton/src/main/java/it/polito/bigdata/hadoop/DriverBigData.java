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

public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		int exitCode;

		Configuration conf = this.getConf();

		Path inputPath;
		Path outputDir;
		inputPath = new Path(args[1]);
		outputDir = new Path(args[2]);
		int numberOfReducersJob1;
		numberOfReducersJob1 = Integer.parseInt(args[0]);

		Job job = Job.getInstance(conf);
		job.setJobName("Lab#4 - Ex.1 - step 1");
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setJarByClass(DriverBigData.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MapperBigData1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductReviewWritable.class);

		// Set reduce class
		job.setReducerClass(ReducerBigData1.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(numberOfReducersJob1);

		// Execute the first job and wait for completion
		if (job.waitForCompletion(true) == true) {
			// Set up the second job
			Job job2 = Job.getInstance(conf);

			// Assign a name to the second job
			job2.setJobName("Lab#3 - Ex.1 - step 2");

			/* */
			// Change the following part of the code
			Path outputDir2;
			int numberOfReducersJob2;
			outputDir2 = new Path(args[4]);

			// Set path of the input file/folder for this second job
			// The output of the first job is the input of this second job    
			FileInputFormat.addInputPath(job2, outputDir);

			// Set path of the output folder for this job
			FileOutputFormat.setOutputPath(job2, outputDir2);

			// Specify the class of the Driver for this job
			job2.setJarByClass(DriverBigData.class);

			// Set job input format
			job2.setInputFormatClass(...);

			// Set job output format
			job2.setOutputFormatClass(...);

			// Set map class
			job2.setMapperClass(MapperBigData2.class);

			// Set map output key and value classes
			job2.setMapOutputKeyClass(...);
			job2.setMapOutputValueClass(...);

			// Set reduce class
			job2.setReducerClass(ReducerBigData2.class);

			// Set reduce output key and value classes
			job2.setOutputKeyClass(...);
			job2.setOutputValueClass(...);

			// Set the number of reducers of the second job 
			numberOfReducersJob2 = ..;
			job2.setNumReduceTasks(numberOfReducersJob2);

			// Execute the second job and wait for completion
			if (job2.waitForCompletion(true) == true)
				exitCode = 0;
			else
				exitCode = 1;
		} else
			exitCode = 1;

		return exitCode;

	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop
		// application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}
