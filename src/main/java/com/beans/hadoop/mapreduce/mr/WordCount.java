package com.beans.hadoop.mapreduce.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.GzipCodec;
//import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;

public class WordCount extends Configured implements Tool{

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
	
	//操作机上运行
	public int run(String[] args) throws Exception {
		//从父类中获取集群的配置信息
		Configuration conf = getConf();
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		//定义job名字并设置任务配置，Configuration赋给job
		Job job = Job.getInstance(conf,"WordCount");
		//设置job运行的类，告诉集群main方法所在的类，WordCount赋给job,实际是赋给job下的Configuration
		job.setJarByClass(WordCount.class);
		//WordCountMapper赋给job,实际是赋给job下的Configuration
		job.setMapperClass(WordCountMapper.class);
		//WordCountReducer赋给job,实际是赋给job下的Configuration
		job.setReducerClass(WordCountReducer.class);
		//设置Combiner
		job.setCombinerClass(WordCountCombiner.class);
		//设置任务的输出key
		job.setOutputKeyClass(Text.class);
		//设置任务的输出value
		job.setOutputValueClass(LongWritable.class);
		//设置任务的输入地址，可以是具体的文件，或者文件所在的目录。
		FileInputFormat.addInputPath(job, new Path(Constants.BASE_PATH+"/input/input.txt"));
		//设置任务的输出地址，对应的是一个目录
		Path outPath = new Path(Constants.BASE_PATH+"/output/wordcount");
//		//开启输出文件压缩
//		FileOutputFormat.setCompressOutput(job, true);
//		//设置输出文件压缩格式
//		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		FileOutputFormat.setOutputPath(job, outPath);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
			
		//等待Job执行完成
		int i= job.waitForCompletion(true)?0:1;
		//任务完成之后，打印Counter
		System.out.println("Counter:"+job.getCounters().getGroup("Frank Line").findCounter("Total").getValue());
		return i;
	}
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new WordCount(),args);
	}

}
