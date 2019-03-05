package com.beans.hadoop.mapreduce.mrcontrol;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.mrcontrol.WordCount.WordCountMapper;
import com.beans.hadoop.mapreduce.mrcontrol.WordCount.WordCountReducer;
import com.beans.hadoop.mapreduce.mrcontrol.WordMax.MaxWordMapper;
import com.beans.hadoop.mapreduce.mrcontrol.WordMax.MaxWordReducer;
import com.beans.hadoop.mapreduce.util.Constants;
import com.beans.hadoop.mapreduce.util.JobRunnerUtil;

public class WordCountMax extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountMax(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		final JobControl jobControl = new JobControl("wordcountandmax");
		ControlledJob wordCountJobCon = getWordCountJob(conf);
		ControlledJob wordMaxJobCon = getWordMaxJob(conf);
		
		wordMaxJobCon.addDependingJob(wordCountJobCon);
		jobControl.addJob(wordCountJobCon);
		jobControl.addJob(wordMaxJobCon);
		
		JobRunnerUtil.run(jobControl);
		
		return 0;
	}
	
	private ControlledJob getWordMaxJob(Configuration conf) throws IOException{
		//复制共用配置到自己的配置对象中，因为Configuration是Iterable的实现所以能被 用到foreach
		Configuration maxConf = new Configuration();
		for(Entry<String, String> entry:conf){
			maxConf.set(entry.getKey(), entry.getValue());
		}
		
		Job maxJob = Job.getInstance(maxConf,"maxword");
		maxJob.setJarByClass(WordMax.class);
		maxJob.setMapperClass(MaxWordMapper.class);
		maxJob.setReducerClass(MaxWordReducer.class);
		maxJob.setMapOutputKeyClass(Text.class);
		maxJob.setMapOutputValueClass(NullWritable.class);
		maxJob.setOutputKeyClass(Text.class);
		maxJob.setOutputValueClass(LongWritable.class);
		//设置reduce个数
		maxJob.setNumReduceTasks(1);
		FileInputFormat.addInputPath(maxJob, new Path(Constants.BASE_PATH+"/input/part-r-00000"));
		Path outputDir = new Path(Constants.BASE_PATH+"/output/maxword");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(maxJob, outputDir);
		
		ControlledJob maxJobCon = new ControlledJob(conf);
		maxJobCon.setJob(maxJob);
		
		return maxJobCon;
	}
	
	private ControlledJob getWordCountJob(Configuration conf) throws IOException{
		Configuration wordCountConf = new Configuration();
		for(Entry<String, String> entry:conf){
			wordCountConf.set(entry.getKey(), entry.getValue());
		}
		
		Job countJob = Job.getInstance(wordCountConf,"wordcount");
		countJob.setJarByClass(WordCountMax.class);
		countJob.setMapperClass(WordCountMapper.class);
		countJob.setReducerClass(WordCountReducer.class);
		
		countJob.setOutputKeyClass(Text.class);
		//设置任务的输出value
		countJob.setOutputValueClass(LongWritable.class);
		//设置任务的输入地址，可以是具体的文件，或者文件所在的目录。
		FileInputFormat.addInputPath(countJob, new Path(Constants.BASE_PATH+"/input/input.txt"));
		//设置任务的输出地址，对应的是一个目录
		Path outPath = new Path(Constants.BASE_PATH+"/output/wordcount");
		FileOutputFormat.setOutputPath(countJob, outPath);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		
		ControlledJob wordCountJobCon = new ControlledJob(conf);
		wordCountJobCon.setJob(countJob);
		
		return wordCountJobCon;
	}

}
