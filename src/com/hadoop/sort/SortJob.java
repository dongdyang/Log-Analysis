package com.hadoop.sort;


import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.log.MainJob;

public class SortJob {

	public void begin(Configuration conf, Path output_tmp, Path output) throws IOException, ClassNotFoundException, InterruptedException {

		Job sortJob = Job.getInstance(conf, "sort");
		sortJob.setJarByClass(MainJob.class);
		sortJob.setInputFormatClass(SequenceFileInputFormat.class);

		sortJob.setMapperClass(InverseMapper.class);
		sortJob.setNumReduceTasks(1); // 限定输出1个
		
		FileInputFormat.addInputPath(sortJob, output_tmp);
		
		FileOutputFormat.setOutputPath(sortJob, output);

		sortJob.setOutputKeyClass(IntWritable.class);
		sortJob.setOutputValueClass(Text.class);
		sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		sortJob.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
		System.out.println("---------------Sort job DONE");
	}
}

class IntWritableDecreasingComparator extends IntWritable.Comparator {
	public int compare(Object a, Object b) {
		return -super.compare(a, b);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return -super.compare(b1, s1, l1, b2, s2, l2);
	}
}
