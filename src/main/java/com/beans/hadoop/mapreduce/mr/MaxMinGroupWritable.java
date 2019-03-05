package com.beans.hadoop.mapreduce.mr;

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

import com.beans.hadoop.mapreduce.util.Constants;
import com.beans.hadoop.mapreduce.writable.WordWritable;

/**
 * 自定义序列化类的使用
 * @author Frank
 *
 */
public class MaxMinGroupWritable extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MaxMinGroupWritable(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"MaxMinGroupWritable");
		job.setJarByClass(MaxMinGroupWritable.class);
		job.setMapperClass(MaxMinGroupWritableMapper.class);
		job.setReducerClass(MaxMinGroupWritableReducer.class);
		job.setCombinerClass(MaxMinGroupWritableCombiner.class);
		job.setMapOutputValueClass(WordWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputDir = new Path(Constants.BASE_PATH+"/input/inputgroup.txt");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/maxmingroupwritable");
		FileInputFormat.addInputPath(job, inputDir);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir,true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}

	private static class MaxMinGroupWritableMapper extends Mapper<LongWritable,Text,Text,WordWritable>{

		private Text outKey = new Text();
		private WordWritable outValue = new WordWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException,InterruptedException {
			String str = value.toString();
			String strs[] = str.split(" ");
			if(strs.length != 2){
				return;
			}
			String word = strs[0];
			Long num = Long.parseLong(strs[1]);
			outKey.set(word);
			outValue.setN(num);
			outValue.setWord(outKey);
			context.write(outKey, outValue);
		}
	}
	
	private static class MaxMinGroupWritableReducer extends Reducer<Text,WordWritable,Text,Text>{

		
		private Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<WordWritable> value,
				Context context) throws IOException, InterruptedException {
			
			long max = 0L;
			long min = 0L;
			//没有使用combiner的实现
//			for(WordWritable wordWritable:value){
//				long num = wordWritable.getN();
//				if(num>max){
//					max = num;
//				}
//				if(min == 0){
//					min = num;
//				}else if(num < min){
//					min = num;
//				}
//			}
			
			//使用Combiner
			for(WordWritable wordWritable:value){
				long num = wordWritable.getN();
				String type = wordWritable.getType();
				if(type.equalsIgnoreCase("max")){
					if(num > max){
						max = num;
					}
				}else {
					if(min == 0){
						min = num;
					} else if(num < min) {
						min = num;
					}
				}
			}
			
			outValue.set(max+"\001"+min);
			context.write(key, outValue);
			

		}
		
	}
	
	/**
	 * 计算每个map任务对应的split中的每组key中的最大值和最小值，用于reducer中进行最大值和最小值
	 * @author Frank
	 *
	 */
	private static class MaxMinGroupWritableCombiner extends Reducer<Text,WordWritable,Text,WordWritable>{
		private WordWritable outValue = new WordWritable();
		@Override
		protected void reduce(Text key, Iterable<WordWritable> value,
				Context context) throws IOException, InterruptedException {
			long max = 0L;
			long min = 0L;
			for(WordWritable wordWritable:value){
				long num = wordWritable.getN();
				if(num > max){
					max = num;
				}
				if(min == 0){
					min = num;
				} else if(num < min){
					min = num;
				}
			}
			
			outValue.setType("min");
			outValue.setN(min);
			context.write(key, outValue);
			outValue.setType("max");
			outValue.setN(max);
			context.write(key, outValue);
		}
	}
}
