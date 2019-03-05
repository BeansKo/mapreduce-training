package com.beans.hadoop.mapreduce.mrmutil;

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

import com.beans.hadoop.mapreduce.base.BaseMR;
import com.beans.hadoop.mapreduce.util.Constants;

/*
 * 排重
 */
public class DistinctWord extends BaseMR{
	public static class DistinctWordMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		Text outKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String valueStr = value.toString();
			String valueArr[] = valueStr.split(" ");
			for(String s:valueArr){
				outKey.set(s);
				context.write(outKey, NullWritable.get());
			}
			context.getCounter("WordDiscinct", "read line").increment(1);
		}
	}
	
	public static class DistinctWordReducer extends Reducer<Text,NullWritable,Text,NullWritable>{

		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
			context.getCounter("WordDiscinct","word num").increment(1);
		}
	}

	@Override
	public Job getJob(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf,"DistinctWord");
		job.setJarByClass(DistinctWord.class);
		job.setMapperClass(DistinctWordMapper.class);
		job.setReducerClass(DistinctWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		Path inputDir = new Path(Constants.BASE_PATH+"/input/input.txt");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/worddistinct");
		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job;
	}

	@Override
	public String getJobName() {
		return "DistinctWord";
	}
}
