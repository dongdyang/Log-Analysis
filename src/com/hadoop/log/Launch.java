package com.hadoop.log;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hadoop.FileOperation.FileOperation;
import com.hadoop.sort.SortJob;

public class Launch {
	static Configuration conf = new Configuration();

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// conf.set("fs.default.name", "hdfs://localhost:9000");

		FileSystem fs = FileSystem.get(conf);
		if (args.length < 2) {
			System.out.println("using default input file");
			args = new String[] { "/user/hadoop/behavior", "/user/hadoop/output" };
		}
		Path input = new Path(args[0]);
		Path input_tmp = new Path("/user/hadoop/input_tmp");
		Path output = new Path(args[1]);
		Path output_tmp = new Path("/user/hadoop/output_tmp");
//
//		if (fs.exists(input_tmp)) {
//			fs.delete(input_tmp, true);
//		}
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		if (fs.exists(output_tmp)) {
			fs.delete(output_tmp, true);
		}
//		Date startTime = new Date();
//		System.out.println("Job started: " + startTime);
//		FileOperation fileOperation = new FileOperation();
//		fileOperation.FileRelative(input, input_tmp, fs);
//		Date end_time = new Date();
//		System.out.println("Job ended: " + end_time);
//		System.out.println("The merge took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
//		
		MainJob mainJob = new MainJob();
		mainJob.begin(conf, input_tmp, output_tmp);
		SortJob sortJob = new SortJob();
		sortJob.begin(conf, output_tmp, output);
	}
}
