package com.beans.hadoop.mapreduce.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * 任务工作链运用工具类
 * @author Frank
 *
 */
public class JobRunnerUtil {
	
	private static ExecutorService executorService = Executors.newFixedThreadPool(1);
	
	public static JobRunnerResult run(JobControl jobc) throws Exception{
		/**
		 * 启动任务工作链
		 */
		Thread t = new Thread(jobc);
		t.start();
		/**
		 * 监控任务工作链运行，并得到运行结果，并阻塞程序退出，直达mr全部运行完成
		 */
		Future<JobRunnerResult> future = executorService.submit(new JobCallable(jobc));
		return future.get();
	}
	
	public static class JobCallable implements Callable<JobRunnerResult>{
		private JobControl jobc;
		
		public JobCallable(JobControl jobc) {
			this.jobc = jobc;
		}
		@Override
		public JobRunnerResult call() throws Exception {
			long startTime = System.currentTimeMillis();
			//所有任务没有运行完成就一直阻塞
			while(!this.jobc.allFinished()){
				Thread.sleep(1000L);
			}
			
			JobRunnerResult jrr = new JobRunnerResult();
			//拿到所有成功任务的counters
			for(ControlledJob job:this.jobc.getSuccessfulJobList()){
				jrr.setCounterMap(job.getJobName(), job.getJob().getCounters());
			}
			//根据失败任务判断整个工作 链是否成功
			jrr.setSuccess(jobc.getFailedJobList().size()==0);
			long endTime = System.currentTimeMillis();
			long runTime = endTime - startTime;
			//计算运行时间
			jrr.setRunTime(getLifeTime(runTime));
			//打印工作链任务状态和运行时间
			System.out.println("JOB_"+(jrr.isSuccess()?"SUCESS":"FAILD"));
			System.out.println(jrr.getRunTime());
			//停止任务链
			jobc.stop();
			return jrr;
		}
		
		/**
		 * 计算剩余毫秒对应的时间
		 * @param mss
		 * @return      XX天xx小时xx秒
		 */
		private String getLifeTime(long mss){
			long days = mss/(1000*60*60*24);
			long hours = (mss%(1000*60*60*24))/(1000*60*60);
			long minutes = (mss%(1000*60*60))/(1000*60);
			long seconds = (mss%(1000*60))/1000;
			
			StringBuilder sb = new StringBuilder();
			if(days != 0){
				sb.append(days).append("天");
			}
			if(hours != 0){
				sb.append(hours).append("小时");
			}
			if(minutes != 0){
				sb.append(minutes).append("分");
			}
			if(seconds != 0){
				sb.append(seconds).append("秒");
			}
			
			return sb.toString();
		}
	}
}
