package com.hadoop.log2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value.charAt(0) == 'T') {
			String[] strs = value.toString().split("\\[=\\]");
			if (strs.length > 3) {
				try {
					if (strs[3].charAt(0) == 'U') {
						String[] tmp = strs[3].substring(4).split("//");
						String website = new String();
						if (tmp.length > 1) {
							String[] tmp2 = tmp[1].split("/");
							if (tmp2.length > 0) {
								website = tmp2[0];
							} else {
								website = "?";
							}
						} else {
							website = strs[3].substring(4).split("/")[0];
						}
						//String explore = strs[1].substring(4);
						//word.set(explore.toLowerCase() + " " + website);
						word.set(website);
						context.write(word, one);
					}
				} catch (Exception e) {
					System.out.println(value + "-------------------index wrong!");
					//Thread.sleep(10000);
				}
			}
		}
	}
}
