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

public class WordCountMax2 extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountMax2(), args));
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
		
		
		Thread thThread = new Thread(){

			@Override
			public void run() {
				//有任务没有运行完成就让程序阻塞，防止这个线程直接退出，因为不阻塞就会调用到stop方法那整个任务工作链就停止运行了
				while(!jobControl.allFinished()){
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//得到运行完成之后的失败任务数，如果一个没有，表示任务全部运行成功，然后记录任务整体的运行状态
				Boolean jobSuccess = false;
				if(jobControl.getFailedJobList().size() == 0){
					jobSuccess = true;
				}
				if(jobSuccess){
					System.out.println("job_:"+jobSuccess);
				}
				//停止工作链运行
				jobControl.stop();
			}
		};
		
		//启动线程，监控任务
		thThread.start();
		//提交任务并阻塞运行执行，因为run方法是个死循环，必须在外界通知结束条件才退出
		jobControl.run();
		
		System.out.println(wordMaxJobCon.getJob().getCounters());
		System.out.println(wordMaxJobCon);
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
		countJob.setJarByClass(WordCountMax2.class);
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
