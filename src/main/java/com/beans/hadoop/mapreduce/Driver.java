package com.beans.hadoop.mapreduce;

import org.apache.hadoop.util.ProgramDriver;

import com.beans.hadoop.mapreduce.mr.WordCount;


public class Driver {

	public static void main(String[] args) throws Throwable {
		ProgramDriver  driver = new ProgramDriver();
		driver.addClass("wordcount", WordCount.class, "词数");
		
		driver.run(new String[]{"wordcount"});
		
//		ProgramDriver.class.getMethod("driver", new Class[] {String.class}).invoke(driver, "wordcount");
	}

}
