package com.beans.hadoop.mapreduce.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordMax extends BaseMR{

	private static final String SING1="\t";
	private static final String SING2="\001";

	public static class MaxWordMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

		private Text wordOut = new Text();
		private long max = 0L;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str[] = value.toString().split(SING1);
			if(str.length != 2){
				context.getCounter("Frank","bad line").increment(1);
				return;
			}
			
			String word = str[0];
			Long count = Long.parseLong(str[1]);
			
			if(count>max){
				max = count;
				wordOut.set(word+SING2+count);
			}
		}
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(wordOut, NullWritable.get());
		}
	}
	
	public static class MaxWordReducer extends Reducer<Text,NullWritable,Text,LongWritable>{

		private Text outKey = new Text();
		private LongWritable outValue = new LongWritable();
		private long max = 0L;
		@Override
		protected void reduce(Text keyIn, Iterable<NullWritable> valueIn,Context context) throws IOException, InterruptedException {
			String wordNum = keyIn.toString();
			String strs[] = wordNum.split(SING2);
			if(strs.length != 2){
				context.getCounter("Frank","bad line").increment(1);
				return;
			}
			String word = strs[0];
			Long num = Long.parseLong(strs[1]);
			if(num > max){
				max = num;
				outKey.set(word);
				outValue.set(max);
			}
			
			context.write(outKey, outValue);
		}
	}

	@Override
	public Job getJob(Configuration conf) throws Exception {
		Job maxJob = Job.getInstance(conf,getJobNameWithTaskId());
		maxJob.setJarByClass(WordMax.class);
		maxJob.setMapperClass(MaxWordMapper.class);
		maxJob.setReducerClass(MaxWordReducer.class);
		maxJob.setMapOutputKeyClass(Text.class);
		maxJob.setMapOutputValueClass(NullWritable.class);
		maxJob.setOutputKeyClass(Text.class);
		maxJob.setOutputValueClass(LongWritable.class);
		//设置reduce个数
		maxJob.setNumReduceTasks(1);
		WordCount wc = new WordCount();
		FileInputFormat.addInputPath(maxJob, getPath(wc.getJobNameWithTaskId()));
		Path outputDir = getPath(getJobNameWithTaskId());
		FileOutputFormat.setOutputPath(maxJob, outputDir);

		return maxJob;
	}

	@Override
	public String getJobName() {
		return "wordmax";
	}
}
