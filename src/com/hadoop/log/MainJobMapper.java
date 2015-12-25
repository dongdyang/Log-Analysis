package com.hadoop.log;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MainJobMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value.charAt(0) == 'T') {
			String[] strs = value.toString().split("\\[=\\]");
			if (strs.length > 1) {
				word.set(strs[1].substring(4).toLowerCase());
				context.write(word, one);
			}
		} else {
		}
	}
}

/*
 * public void map(K1 key, V1 value, OutputCollector<K2,V2> output ,Reporter
 * reporter ) throws IOException 或者 public void map(K1 key, V1 value, Context
 * context) throws IOException, InterruptedException 该函数处理一个给定的键/值对(K1,
 * V1)，生成一个键/值对(K2, V2)的列表（该列表也可能为空）。
 *
 * 
 * StringTokenizer itr = new StringTokenizer(value.toString()); while
 * (itr.hasMoreTokens()) { word.set(itr.nextToken()); context.write(word, one);
 * }
 * 
 */
