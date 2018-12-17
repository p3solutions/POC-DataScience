package com.learn.logging.logger;

import java.io.IOException;
import java.util.Date;
import java.util.Enumeration;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class JobLogger {
	
	public static String LOG4JPATH = "/log.properties";

	private static JobLogger JobLogger = new JobLogger();
	// private static CoreSettings properties = new
	// CoreSettings("C://JOBS//lib//utility.properties");
	private static CoreSettings properties = new CoreSettings(LOG4JPATH,"CLASS");

	private static Logger logger = null;

	/**
	 * This static block will be executed the first time JobLogger is
	 * constructed/accessed in any way. This block simply calls the setupLogger
	 * helper method to perform the actual init of logging.
	 */
	static {
		setupLogger();
	}

	/**
	 * Returns a static instance of this class the application can use to access the
	 * log methods.
	 */
	public static JobLogger getLogger() {
		return JobLogger;
	}

	/**
	 * This method performs the initialization of the logging facility. A daily
	 * rolling log file scheme is used. The following are configurable in the
	 * droc.properties file: JobLogger.LogFile - Full path and file name of log
	 * file. Date and time will be appended automatically on roll-over.
	 * JobLogger.LogRollingInterval - A log4j interval pattern such as '.'yyyy-MM-dd
	 * See log4j for more details. JobLogger.LogLevel - The level of which logging
	 * should be performed at. JobLogger.LogInstanceName - A unique name used
	 * internally by log4j for this instance of the logger.
	 */
	@SuppressWarnings({ "rawtypes", "static-access", "deprecation" })
	public static void setupLogger() {
		String JobLoggerErrorLogFile = null;
		String JobLoggerErrorLogRollingInterval = null;
		String JobLoggerErrorLogLevel = null;
		String JobLoggerErrorLogName = null;
		try {
			JobLoggerErrorLogFile = properties.getStringValue("app.logFile");
			JobLoggerErrorLogRollingInterval = properties.getStringValue("app.logRollingInterval");
			JobLoggerErrorLogLevel = properties.getStringValue("app.logLevel");
			JobLoggerErrorLogName = properties.getStringValue("app.logInstanceName");
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		JobLogger.logger = Logger.getLogger(JobLoggerErrorLogName);
		PatternLayout pl = new PatternLayout();
		pl.setConversionPattern("%d{yyyy/MM/dd:HH:mm:ss,SSS}|%-7p%m%n");
		DailyRollingFileAppender dr = null;
		try {
			dr = new DailyRollingFileAppender(pl, JobLoggerErrorLogFile, JobLoggerErrorLogRollingInterval);
		} catch (IOException ioe) {
			System.out.println(ioe);
		}
		boolean addDrocAppender = true;
		Enumeration e = JobLogger.logger.getAllAppenders();
		if (e != null) {
			while (e.hasMoreElements()) {
				FileAppender app = (FileAppender) e.nextElement();
				if ((app.getFile() != null) && (app.getFile().equals(JobLoggerErrorLogFile))) {
					addDrocAppender = false;
				}
			}
		}

		if (addDrocAppender)
			JobLogger.logger.addAppender(dr);

		if (JobLoggerErrorLogLevel.equalsIgnoreCase("DEBUG"))
			JobLogger.logger.setLevel(Level.DEBUG);
		else if (JobLoggerErrorLogLevel.equalsIgnoreCase("INFO"))
			JobLogger.logger.setLevel(Level.INFO);
		else if (JobLoggerErrorLogLevel.equalsIgnoreCase("WARN"))
			JobLogger.logger.setLevel(Level.WARN);
		else if (JobLoggerErrorLogLevel.equalsIgnoreCase("OFF"))
			JobLogger.logger.setLevel(Level.OFF);
		else if (JobLoggerErrorLogLevel.equalsIgnoreCase("ERROR"))
			JobLogger.logger.setLevel(Level.ERROR);
		else if (JobLoggerErrorLogLevel.equalsIgnoreCase("ALL"))
			JobLogger.logger.setLevel(Level.ALL);
		else if (JobLoggerErrorLogLevel.equalsIgnoreCase("FATAL"))
			JobLogger.logger.setLevel(Level.FATAL);

		JobLogger.logger.info(
				"-----------------------------" + new Date().toLocaleString() + "------------------------------------");
		JobLogger.logger.info("Logging initialized...");
	}

	/**
	 * Logs a line at the DEBUG level.
	 */
	public void debug(String cls, String mtd, String msg) {
		logger.debug(formatMessage(cls, mtd, msg));
	}

	/**
	 * Logs a line at the INFO level.
	 */
	public void info(String cls, String mtd, String msg) {
		logger.info(formatMessage(cls, mtd, msg));
	}

	/**
	 * Logs a line at the WARN level.
	 */
	public void warn(String cls, String mtd, String msg) {
		logger.warn(formatMessage(cls, mtd, msg));
	}

	/**
	 * Logs a line at the ERROR level.
	 */
	public void error(String cls, String mtd, String msg) {
		logger.error(formatMessage(cls, mtd, msg));
	}

	/**
	 * Logs a line at the FATAL level.
	 */
	public void fatal(String cls, String mtd, String msg) {
		logger.fatal(formatMessage(cls, mtd, msg));
	}

	/**
	 * Takes the data from the log methods into standard format.
	 */
	private String formatMessage(String cls, String mtd, String msg) {
		return " | " + padding(cls, 50) + padding(mtd, 25) + msg;
	}

	private String padding(String mtd, int length) {
		if (mtd == null)
			return padRight("", length);
		return padRight(mtd, length);
	}

	public static String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}

	public static String padLeft(String s, int n) {
		return String.format("%1$" + n + "s", s);
	}

	/**
	 * Closes down log4j.
	 */
	public void close() {
		LogManager.shutdown();
	}

}
