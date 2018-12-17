package com.learn.logging;

import com.learn.logging.logger.JobLogger;

public class LoggerMain {

	public static void main(String[] args) {
		System.out.println("start method");
		
		JobLogger.getLogger().info(LoggerMain.class.getName(), "Main Method" , "check logger");
		
		System.out.println("end method");
	}
}
