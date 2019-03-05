package com.beans.hadoop.mapreduce.base;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import com.beans.hadoop.mapreduce.util.Constants;

public abstract class BaseMR {
	public static Configuration configuration;
	
	public void setConf(Configuration conf){
		BaseMR.configuration = conf;
	}

	public ControlledJob getControlledJob() throws Exception {
		FileSystem fSystem = FileSystem.get(configuration);
		Path outputPath = getPath(getJobNameWithTaskId());
		if (fSystem.exists(outputPath)) {
			fSystem.delete(outputPath, true);
			System.out.println("out put delete finish.");
		}

		Configuration jobConf = new Configuration();
		for (Entry<String, String> entry : configuration) {
			jobConf.set(entry.getKey(), entry.getValue());
		}
		ControlledJob cJob = new ControlledJob(configuration);
		Job job = getJob(jobConf);
		cJob.setJob(job);

		return cJob;
	}

	public abstract Job getJob(Configuration conf) throws Exception;

	/**
	 * 任务名称
	 * @return
	 */
	public abstract String getJobName();

	/**
	 * 任务ID组合生成的任务名称
	 * @return
	 */
	public String getJobNameWithTaskId() {
		return getJobName() + "_" + configuration.get(Constants.TASK_ID);
	}


	/**
	 * 任务的工作路径
	 * @return
	 */
	public String getWorkPath() {
		return Constants.TASK_WORK_BASE_PATH;
	}

	/**
	 * 根据任务名称得到本任务的输入或输出地址
	 * @param jobName
	 * @return
	 */
	public Path getPath(String jobName) {
		return new Path(getWorkPath() + jobName);

	}
}
