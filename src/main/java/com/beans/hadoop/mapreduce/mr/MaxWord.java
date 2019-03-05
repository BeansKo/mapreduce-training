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

/*
 * 全局最大值
 */
public class MaxWord extends Configured implements Tool{

	private static final String SING1="\t";
	private static final String SING2="\001";
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"maxword");
		job.setJarByClass(MaxWord.class);
		job.setMapperClass(MaxWordMapper.class);
		job.setReducerClass(MaxWordReducer.class);
		//当map的输出数据类型和reducer的输出数据类型不一致的时候，需要给map单独制定输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//设置reduce个数
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(Constants.BASE_PATH+"/input/part-r-00000"));
		Path outputDir = new Path(Constants.BASE_PATH+"/output/maxword");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MaxWord(), args);
	}

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
}
