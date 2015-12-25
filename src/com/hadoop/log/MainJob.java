package com.hadoop.log;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MainJob {

	public void begin(Configuration conf, Path input_tmp, Path output_tmp)
			throws IOException, InterruptedException, ClassNotFoundException {
		// first job
		Job job = Job.getInstance(conf, "log analysis");
		job.setJarByClass(MainJob.class);
		job.setMapperClass(MainJobMapper.class);
		// job.setCombinerClass(QuestionReducer.class);
		job.setReducerClass(MainJobReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, input_tmp);
		FileOutputFormat.setOutputPath(job, output_tmp);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");

		System.out.println("---------------Main job DONE");
	}

}

