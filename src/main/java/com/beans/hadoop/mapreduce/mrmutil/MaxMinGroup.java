package com.beans.hadoop.mapreduce.mrmutil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.base.BaseMR;
import com.beans.hadoop.mapreduce.util.Constants;

/*
 * 同时求每组词的最大值最小值
 */
public class MaxMinGroup extends BaseMR{

	private static class MaxMinGroupMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

		private Text outKey = new Text();
		private LongWritable outValue = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String strs[] = str.split(" ");
			if(strs.length != 2){
				return;
			}
			String word = strs[0];
			Long num = Long.parseLong(strs[1]);
			outKey.set(word);
			outValue.set(num);
			context.write(outKey, outValue);
		}
		
	}
	
	private static class MaxMinGroupReducer extends Reducer<Text,LongWritable,Text,Text>{

		private Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> value,
				Context context) throws IOException, InterruptedException {
			long max=0L;
			long min=0L;
			for(LongWritable nums:value){
				long num = nums.get();
				if(num > max){
					max = num;
				}
				if(min == 0){
					min = num;
				}else if(num < min){
					min = num;
				}
			}
			outValue.set(max+"\001"+min);
			context.write(key, outValue);
		}
		
	}

	@Override
	public Job getJob(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf,"MaxMinGroup");
		job.setJarByClass(MaxMinGroup.class);
		job.setMapperClass(MaxMinGroupMapper.class);
		job.setReducerClass(MaxMinGroupReducer.class);
		//map和reduce的输入key类型相同，value不同，所以只需要指定value类型
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputDir = new Path(Constants.BASE_PATH+"/input/inputgroup.txt");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/maxmingroup");
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job;
	}

	@Override
	public String getJobName() {
		return "MaxMinGroup";
	}
}
