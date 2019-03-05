package com.beans.hadoop.mapreduce.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beans.hadoop.mapreduce.util.Constants;

public class WordCountMax extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountMax(), args));
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set(Constants.TASK_ID, "frank");
		final JobControl jobControl = new JobControl("wordcountandmax");
		
		WordCount wc = new WordCount();
		wc.setConf(conf);
		WordMax wm = new WordMax();
		
		ControlledJob wordCountJobCon = wc.getControlledJob();
		ControlledJob wordMaxJobCon = wm.getControlledJob();
		
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
}
