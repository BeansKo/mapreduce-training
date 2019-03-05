package com.beans.hadoop.mapreduce.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.beans.hadoop.mapreduce.mrcontrol.WordCountMax;
import com.beans.hadoop.mapreduce.util.Constants;

public class WordCount extends BaseMR{

	//集群上运行
	public static class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		LongWritable valueOut = new LongWritable(1);
		Text keyOut = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			context.getCounter("Frank Line","Total").increment(1);
			String[] split = value.toString().split(" ");
			for(String str:split){
				if (str.contains("a")){
					context.getCounter("Frank Line","a Number").increment(1);
				}
				keyOut.set(str);
				context.write(keyOut, valueOut);
			}
		}	
	}
	
	//集群上运行
	public static class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		LongWritable outValue = new LongWritable();
		@Override
		protected void reduce(Text keyIn, Iterable<LongWritable> valueIn,Context context) throws IOException, InterruptedException {
			long sum = 0L;
			for(LongWritable num:valueIn){
				sum += num.get();
			}
			                                                                                                      
			outValue.set(sum);
			context.getCounter("Word", "Word count").increment(1);
			context.write(keyIn, outValue);
		}
	}
	
	public static class WordCountCombiner extends Reducer<Text,LongWritable,Text,LongWritable>{

		LongWritable outValue = new LongWritable();
		@Override
		protected void reduce(Text keyIn, Iterable<LongWritable> valueIn,Context context) throws IOException, InterruptedException {
			long sum = 0L;
			for(LongWritable num:valueIn){
				sum += num.get();
			}
			                                                                                                      
			outValue.set(sum);
			context.write(keyIn, outValue);
		}
	}

	@Override
	public Job getJob(Configuration conf) throws Exception {
		Job countJob = Job.getInstance(conf,getJobNameWithTaskId());
		countJob.setJarByClass(WordCountMax.class);
		countJob.setMapperClass(WordCountMapper.class);
		countJob.setReducerClass(WordCountReducer.class);
		
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(LongWritable.class);
		//临时这样写
		conf.set(Constants.TASK_INPUT, Constants.BASE_PATH+"/input/input.txt");
		FileInputFormat.addInputPath(countJob, new Path(conf.get(Constants.TASK_INPUT)));
		FileOutputFormat.setOutputPath(countJob, getPath(getJobNameWithTaskId()));
		
		return countJob;
	}

	@Override
	public String getJobName() {
		return "wordcount";
	}
}
