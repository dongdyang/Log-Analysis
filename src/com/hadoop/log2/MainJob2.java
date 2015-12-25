package com.hadoop.log2;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.hadoop.sort.SortJob;

public class MainJob2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "log analysis 2");

		FileSystem fs = FileSystem.get(conf);
		if (args.length < 2) {
			System.out.println("using default input file");
			args = new String[] { "/user/hadoop/input_tmp", "/user/hadoop/output2" };
		}
		Path input = new Path(args[0]);
		Path output_tmp = new Path("/user/hadoop/output2_tmp");
		Path output = new Path(args[1]);

		// if (fs.exists(input_tmp)) {
		// fs.delete(input_tmp, true);
		// }
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		if (fs.exists(output_tmp)) {
			fs.delete(output_tmp, true);
		}

		job.setJarByClass(MainJob2.class);

		job.setMapperClass(Job2Mapper.class);
		job.setReducerClass(Job2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output_tmp);

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");

		System.out.println("---------------Main job 2 DONE");

		SortJob sortJob = new SortJob();
		sortJob.begin(conf, output_tmp, output);

	}
}
