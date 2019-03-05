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
/*
 * 同时求全局最大最小值
 */
public class MaxMinWord extends Configured implements Tool {
	private static final String SING1 = "\t";
	private static final String SING2 = "\001";

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MaxMinWord(), args));
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"MaxMinWord");
		job.setJarByClass(MaxMinWord.class);
		job.setMapperClass(MaxMinWordMapper.class);
		job.setReducerClass(MaxMinWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(Constants.BASE_PATH+"/input/part-r-00000"));
		Path outputDir = new Path(Constants.BASE_PATH+"/output/maxminword");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir,true);
		}
		
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}

	public static class MaxMinWordMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text maxWordOut = new Text();
		private Text minWordOut = new Text();
		private long max = 0L;
		private long min = 0L;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String strs[] = value.toString().split(SING1);
			if (strs.length != 2) {
				context.getCounter("Frank", "Max Min bad line").increment(1);
				return;
			}
			String word = strs[0];
			Long wordNum = Long.parseLong(strs[1]);
			if (wordNum > max) {
				max = wordNum;
				maxWordOut.set(word + SING2 + wordNum);
			}
			if (min == 0) {
				min = wordNum;
				minWordOut.set(word + SING2 + wordNum);
			} else if (wordNum < min) {
				min = wordNum;
				minWordOut.set(word + SING2 + wordNum);
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Text minKey = new Text("min");
			Text maxKey = new Text("max");

			context.write(minKey, minWordOut);
			context.write(maxKey, maxWordOut);
		}

	}

	private static class MaxMinWordReducer extends Reducer<Text, Text, Text, Text> {
		private Text outMin = new Text();
		private Text outMax = new Text();
		private Long min = 0L;
		private Long max = 0L;

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context arg2) throws IOException, InterruptedException {
			String type = key.toString();
			if (type.equalsIgnoreCase("min")) {
				for (Text wordNum : value) {
					String temp[] = wordNum.toString().split(SING2);
					String word = temp[0];
					Long num = Long.parseLong(temp[1]);
					if (min == 0) {
						min = num;
						outMin.set(word + SING2 + num);
					} else if (num < min) {
						min = num;
						outMin.set(word + SING2 + num);
					}
				}
			} else {
				for (Text wordNum : value) {
					String temp[] = wordNum.toString().split(SING2);
					String word = temp[0];
					Long num = Long.parseLong(temp[1]);
					if (num > max) {
						max = num;
						outMax.set(word + SING2 + num);
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Text minText = new Text("min");
			Text maxText = new Text("max");
			context.write(minText, outMin);
			context.write(maxText, outMax);
		}
	}
}
