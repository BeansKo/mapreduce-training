package com.beans.hadoop.mapreduce.util;

import java.io.File;

public class Constants {
	
	public static final String BASE_PATH = System.getProperty("user.dir")+ File.separator+"tmp"+File.separator;
	public static final String TASK_ID = "hainiu.task.id";
	public static final String TASK_INPUT = "hainiu.task.input";
	public static final String TASK_WORK_BASE_PATH = "/user/task/";
	
	public static void main(String[] args) {
		System.out.println(BASE_PATH);
	}
}
