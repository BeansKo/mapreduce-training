package com.beans.hadoop.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;

/**
 * word count 倒序输出
 */
public class WordSortDesc extends Configured implements Tool{

	private static class WordSortDescMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

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
	
	private static class WordSortDescReducer extends Reducer<LongWritable, Text, NullWritable, Text>{

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			for(Text v:value){
				context.write(NullWritable.get(), v);
			}
		}
		
	}
	
	private static class LongKeyDescComparator extends WritableComparator{

		@SuppressWarnings("unused")
		public LongKeyDescComparator() {
			super(LongWritable.class,true);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"wordsortasc");
		job.setJarByClass(WordSortDesc.class);
		job.setMapperClass(WordSortDescMapper.class);
		job.setReducerClass(WordSortDescReducer.class);
		job.setSortComparatorClass(LongKeyDescComparator.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//全局排序,多个reduce结果排序会错误
		job.setNumReduceTasks(1);
		Path inputDir = new Path(Constants.BASE_PATH+"/output/wordcount/part-r-00000");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/wordsortdesc");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordSortDesc(), args));
	}
}
