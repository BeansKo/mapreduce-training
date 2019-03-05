package com.beans.hadoop.mapreduce.mrcontrol;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount{

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
}
