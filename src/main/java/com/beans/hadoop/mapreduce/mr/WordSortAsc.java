package com.beans.hadoop.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;

/**
 * 排序
 */
public class WordSortAsc extends Configured implements Tool{

	private static class WordSortAscMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

		private Text outValue = new Text();
		private LongWritable outKey = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			String[] strs = str.split("\t");
			if(strs.length != 2){
				return;
			}
			
			Long num = Long.parseLong(strs[1]);
			outKey.set(num);
			outValue.set(str);
			context.write(outKey, outValue);
		}
		
	}
	
	private static class WordSortAscReducer extends Reducer<LongWritable, Text, NullWritable, Text>{

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			for(Text v:value){
				context.write(NullWritable.get(), v);
			}
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"wordsortasc");
		job.setJarByClass(WordSortAsc.class);
		job.setMapperClass(WordSortAscMapper.class);
		//不设置reduce也可以运行，因为父类有个reducer方法
//		job.setReducerClass(Reducer.class);
		job.setReducerClass(WordSortAscReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//全局排序,多个reduce结果排序会错误
		job.setNumReduceTasks(1);
		Path inputDir = new Path(Constants.BASE_PATH+"/output/wordcount/part-r-00000");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/wordsortasc");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordSortAsc(), args));
	}



}
