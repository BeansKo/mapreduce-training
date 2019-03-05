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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;
import com.beans.hadoop.mapreduce.writable.WordWritable;

/**
 * 多目录输出
 * @author Frank
 *
 */
public class MulitipleOutMaxMin extends Configured implements Tool{
//	private static final String SING1 = "\t";
	private static final String SING2 = "\001";
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MulitipleOutMaxMin(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"MulitipleOutMaxMin");
		job.setJarByClass(MulitipleOutMaxMin.class);
		job.setMapperClass(MulitipleOutMaxMinMapper.class);
		job.setReducerClass(MulitipleOutMaxMinReducer.class);
		job.setCombinerClass(MulitipleOutMaxMinCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path inputDir = new Path(Constants.BASE_PATH+"/input/inputgroup.txt");
		Path outputDir = new Path(Constants.BASE_PATH+"/output/mulitipleout");
		FileInputFormat.addInputPath(job, inputDir);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		return job.waitForCompletion(true)?0:1;
	}

	private static class MulitipleOutMaxMinMapper extends Mapper<LongWritable,Text,Text,WordWritable>{

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
	
	private static class MulitipleOutMaxMinReducer extends Reducer<Text,WordWritable,Text,Text>{
		private MultipleOutputs<Text,Text> mos;
		private Text valueOut = new Text();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text,Text>(context);
		}


		@Override
		protected void reduce(Text key, Iterable<WordWritable> value, Context context)
				throws IOException, InterruptedException {
			long max = 0L;
			long min = 0L;
			
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
			
			//多目录输出的使用方法
			valueOut.set(String.valueOf(max));
			//这里maxout是文件的输出目录，max是文件的前缀
			mos.write(key, valueOut, "maxout/max");
			valueOut.set(String.valueOf(min));
			mos.write(key, valueOut, "minout/max");
			valueOut.set(max + SING2+min);
			//多目录输出不影响context的输出，多目录输出只是放到指定的文件夹中
			context.write(key, valueOut);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close(); 
		}
	}
	
	private static class MulitipleOutMaxMinCombiner extends Reducer<Text,WordWritable,Text,WordWritable>{
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