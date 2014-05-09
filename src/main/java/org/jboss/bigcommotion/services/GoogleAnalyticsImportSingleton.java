package org.jboss.bigcommotion.services;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;

import org.apache.commons.lang.StringUtils;
import org.jboss.bigcommotion.model.WebMetric;
import org.jboss.bigcommotion.util.Resources;

/**
 * Imports a CSV formatted Google Analytics page-view report based on standard defaults.
 * TODO:  Refactor the code to make visibility of the site name and metric list.
 */
@Startup
@Singleton
public class GoogleAnalyticsImportSingleton {

	private static final String DATE_FORMAT = "yyyyMMdd";
	private static final String DEFAULT_DATA_PATH = "/opt/data";
	private static File dataPath;

	@PersistenceContext(unitName = Resources.PERSISTENCE_CONTEXT_NAME)
	private EntityManager em;

	@Inject
	private transient Logger logger;	

	@Resource(mappedName = "java:/ConnectionFactory")
	private ConnectionFactory connectionFactory;

	@Resource(mappedName = "java:/" + Resources.PAGEVIEW_QUEUE)
	private Queue queue;

	private HashSet<WebMetric> messageSet = new HashSet<WebMetric>();

	@Inject
	AnalyticsPageViewParser parser;
	
	// -------------------------------------------------------------------

	@PostConstruct
	private void setup(){
		//TODO:  Make the data path configurable.
		dataPath = new File(DEFAULT_DATA_PATH);	
		logger.fine("Google Analytic Scanner now to scan " + dataPath + " for metrics.");
		poll();
	}		


	/**
	 * Poll is the main entry-point into the class.  By default, it searches /opt/data/ for files
	 * to import every three minutes.  This really should be changed for the first few days of each 
	 * month.
	 */
	@Schedule(minute="*/30", hour="*")
	public void poll(){
		logger.log(Level.FINE, "staring poll cycle.");
		// Look through file directories
		File[] dirs = dataPath.listFiles();
		for (File dir : dirs){
			//omit Mac specific .DS_Store file, swp and the processed directory
			if (isCrapFile(dir.getName())){
				continue;
			}
			
			//Assign project name, fileName and siteName
			walkthruDir(dir);				
		}
		sendMessages(messageSet); //sends a list of files to be processed.
		logger.log(Level.FINE, "finished polling.");
	}

	
	/**
	 * Sets the fileName, siteName, date and project name where applicable
	 * @param dir
	 */
	private void walkthruDir(File dir){
		assert dir.isDirectory(): dir.getAbsolutePath() + "is not a directory.";
		File[] files = dir.listFiles();
		for (File file: files){
			if (!isCrapFile(file.getName())){
				logger.fine("Found " + file.getPath());
				if (! isFileAlreadyProcessed(file)){
					try {
						Date startDate = getStartDate(file.getName());;					
						WebMetric metric = new WebMetric();
						metric.setFileName(file.getAbsolutePath());
						metric.setSite(dir.getName());  // this should eventually be changed
						logger.finest("setting " + dir.getName() + "as site");
						if (dir.getName() != "jboss.org"){
							metric.setProject(dir.getName());
							logger.log(Level.ALL, "assigning " + dir.getName() + "as project");
						}
						metric.setDate(startDate);
						messageSet.add(metric);
					} catch (Exception e){
						logger.log(Level.ALL, "Error getting a startDate", e);
					}					
				}
			}
		}
	}


	/**
	 * Removes any extraneous files from evaluation.
	 * @param fileName
	 * @return
	 */
	private boolean isCrapFile(String fileName){
		assert fileName != null: "fileName must not be null";
		if (StringUtils.contains(fileName, "(1)")){
			return true;
		}
		if (StringUtils.endsWith(fileName, ".DS_Store")
				|| StringUtils.endsWith(fileName, ".swp")
				|| StringUtils.endsWith(fileName, "processed")
				|| StringUtils.endsWith(fileName, "archive")
				|| StringUtils.endsWith(fileName, "archived")
				)
			return true;
		else
			return false;
	}


	/**
	 * Checks to see if the File has already been proccessed by the system.
	 * @param file
	 * @return
	 */
	private boolean isFileAlreadyProcessed(File file){
		assert file != null: "file must not be null.";

		//look up the file path to see if we have already processed it in the past.		
		TypedQuery<Long> findByFilePathQuery = em.createQuery("SELECT COUNT(m) FROM WebMetric m where m.fileName = :path", Long.class );
		findByFilePathQuery.setParameter("path", file.getAbsolutePath());
		long fileCount = findByFilePathQuery.getSingleResult();
		logger.info( "AAAAAAAAA  RECORD COUNT: " + fileCount + " for file " + file.getAbsolutePath());		
		if (fileCount > 0 ){
			logger.log(Level.FINEST, "Already processed " + file.getPath() + " ignoring.");			
			return true;
		}
		else {
			logger.log(Level.FINEST, "Have not proccessed " + file.getAbsolutePath() + " processing now...");
			return false;					
		}
	}


	private void sendMessages(HashSet<WebMetric> messages){
		assert messages!=null:"messages must be specified";
		Connection connection = null;
		Session session = null;
		MessageProducer msgProducer = null;
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			msgProducer = session.createProducer(queue);
			connection.start();	
			for(WebMetric m : messages){
				ObjectMessage msg = session.createObjectMessage(m);
				msg.setObject(m);
				logger.log(Level.FINE, "Sending "+ m.getProject() + m.getDate() );
				msgProducer.send(msg);   			   
			}   			
		} catch (Exception e){
			logger.log(Level.ALL, "Error producing a message", e); 
		}
		finally {
			if (connection != null) {
				try {
					connection.close();

				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
	}



	/**
	 *  Scrapes filename for the month of the metrics.  By default, the Google Analytics naming convention is
	 *  similar to "Analytics [site-name] Pages YYYYMMDD-YYMMDD" where the first date is the start of the report and
	 *  the second date is the end of the report.
	 **/
	private Date getStartDate(final String fileName) {
		assert fileName != null && !fileName.isEmpty(): "fileName must be specified.";

		String dateRange = StringUtils.substringAfterLast(fileName, " ");
		String startDateStr = StringUtils.substringBefore(dateRange, "-");
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		Date startDate = null;

		try {
			logger.fine("********** dateRange = " + dateRange);
			logger.fine("********** startDateStr = " + startDateStr);
			startDate = sdf.parse(startDateStr);

		} catch (ParseException pe){
			logger.severe("Cannot determine start date from file named: " + fileName  
					+ ".  Files must conform to the " + DATE_FORMAT + "format.");
		}

		return startDate;
	}

	/**
	 * 
	 * @param fileName
	 * @return
	 */
	@SuppressWarnings("unused")
	private Date getEndDate(final String fileName){
		String dateRange = StringUtils.substringAfterLast(fileName, " ");
		String endDateStr = StringUtils.substringBeforeLast((StringUtils.substringAfter(dateRange, "-")), ".");
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		Date endDate = null;
		try {
			endDate = sdf.parse(endDateStr);
		} catch (ParseException  pe){
			pe.printStackTrace();
		}
		return endDate;
	}
}





