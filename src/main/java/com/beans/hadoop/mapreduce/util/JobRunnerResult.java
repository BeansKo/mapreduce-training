package com.beans.hadoop.mapreduce.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

public class JobRunnerResult {
	/**
	 * 任务运行结果
	 */
	private boolean isSuccess;

	/**
	 * 任务运行时间
	 */
	private String runTime;

	/**
	 * 任务工作链中所有任务对应的counters
	 */
	private Map<String, Counters> counterMap = new HashMap<>();

	public boolean isSuccess() {
		return isSuccess;
	}

	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}

	public String getRunTime() {
		return runTime;
	}

	public void setRunTime(String runTime) {
		this.runTime = runTime;
	}

	public Map<String, Counters> getCounterMap() {
		return counterMap;
	}

	/**
	 * 得到制定任务对应的counters
	 * 
	 * @param jobName
	 *            任务名称
	 * @return 该任务对应的counters
	 */
	public Counters getCounters(String jobName) {
		return this.counterMap.get(jobName);
	}

	public Counters getCounters(ControlledJob controlledJob) {
		return getCounters(controlledJob.getJobName());
	}

	/**
	 * 得到制定任务的counter
	 * @param controlledJob
	 * @param groupName
	 * @param counterName
	 * @return
	 */
	public long getCounterNum(ControlledJob controlledJob, String groupName, String counterName) {
		Counter counter = getCounters(controlledJob).findCounter(groupName, counterName);
		return Utils.isEmpty(counter) ? 0L : counter.getValue();
	}
	
	/**
	 * 得到制定任务的counter值
	 * @param controlledJob
	 * @param groupName
	 * @param counterName
	 * @return
	 */
	public Counter getCounter(ControlledJob controlledJob, String groupName, String counterName) {
		Counter counter = getCounters(controlledJob).findCounter(groupName, counterName);
		return counter;
	}

	/**
	 * 设置每个任务对应的counters
	 * 
	 * @param jobName
	 *            任务名称
	 * @param counters
	 *            该任务对应的counters
	 */
	public void setCounterMap(String jobName, Counters counters) {
		this.counterMap.put(jobName, counters);
	}
}
