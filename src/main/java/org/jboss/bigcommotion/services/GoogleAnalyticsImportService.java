package org.jboss.bigcommotion.services;

import org.apache.commons.lang.StringUtils;
import org.jboss.bigcommotion.model.WebMetric;
import org.jboss.bigcommotion.util.Resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Imports a CSV formatted Google Analytics page-view report based on standard defaults.
 * TODO:  Refactor the code to make visibility of the site name and metric list.
 */
@Stateless
public class GoogleAnalyticsImportService {
	
	private static final String DATE_FORMAT = "yyyyMMdd";


	@PersistenceContext(unitName = Resources.PERSISTENCE_CONTEXT_NAME)
	private EntityManager em;

	@Inject
	private transient Logger logger;

	@Resource(mappedName = "java:/ConnectionFactory")
	private ConnectionFactory connectionFactory;

	@Resource(mappedName = "java:/" + Resources.PAGEVIEW_QUEUE)
	private Queue queue;

   
   
   /**
    * TODO: Break this method and the parse file out from one another.  Potentially send a JMS message
    * and start asynch processing on all files simultaneously
    * Designed to be the main entry point for this Import Service
    * @param siteName
    * @param path
    */

   public void go(final String siteName, final String path){

	   if (siteName == null || siteName.isEmpty()){
		   logger.log(Level.WARNING, "site name is required to process metrics");
		   throw new IllegalArgumentException("siteName must be specified.");
	   }

	   if (path == null || StringUtils.isEmpty(path)){
		   logger.log(Level.WARNING, "path is required to process metrics");
		   throw new IllegalArgumentException("path must be specified.");
	   }


	   File filePath = new File(path);
	   Date startDate = null;
	   HashSet<WebMetric> messageSet = new HashSet<WebMetric>();

	   if (!filePath.isDirectory()){    	   // Process a single file
		   try {
			   startDate = getStartDate(filePath.getName());
		   } catch (Exception e){
			   logger.log(Level.ALL, "Error getting a startDate", e);
		   }

		   WebMetric wm = new WebMetric();
		   wm.setFileName(path);
		   wm.setDate(startDate);
		   wm.setSite(siteName);
		   messageSet.add(wm);   
	   } 
	   
	   else // Process a directory		   
	   {        	
		   File[] files = filePath.listFiles();
		   for (int i = 0; i < files.length; i++){
			   startDate = getStartDate(files[i].getName());

			   //omit Mac specific .DS_Store file, swp and the processed directory
			   if (StringUtils.endsWith(files[i].getName(), ".DS_Store") 
					   || StringUtils.endsWith(files[i].getName(), ".swp")
					   || StringUtils.endsWith(files[i].getName(), "processed")){
				   continue;
			   }
			   WebMetric wm = new WebMetric();
			   wm.setFileName(path + "/" + files[i].getName());
			   wm.setSite(siteName);
			   wm.setDate(startDate);
			   messageSet.add(wm);
		   }        	
	   }

	   sendMessages(messageSet);
   }	


   	//TODO raise a CDI event instead of this mess!
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
   				System.out.println("Sending " + m.toString());
   				msgProducer.send(msg);   			   
   			}   			
   		} catch (Exception e){
   			logger.log(Level.ALL, "Error producing a message", e); 
   		} finally {
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
        	pe.printStackTrace();
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
    
    

//
//    /**
//     * Parses Google Analytics Standard PageView report, consolidates duplicate entries and persists them
//     * to the database
//     * @param file file to be parsed
//     * @param startDate start date of the report
//     * @return
//     * @throws Exception
//     */
//    private Collection<WebMetric> parseFile(File file, Date startDate) throws Exception{
//    	
//    	assert file != null : "file must be specified";
//    	assert startDate !=null : "startDate must be specified";
//        
//    	Map<String, WebMetric> metrics = new HashMap<String, WebMetric>();
//
//        FileReader fileReader = new FileReader(file);
//        BufferedReader bufferedFileReader = new BufferedReader(fileReader);
//    	Scanner scanner = new Scanner(bufferedFileReader);
//		long lineNum = 0;
//		
//        // Skip the metadata for now.
//        while (lineNum < 7){
//            scanner.nextLine();
//        	lineNum++;
//        }
//
//        scanner.useDelimiter(REGEX_COMMAS_AND_QUOTES);
//        
//        // Scan each line.  
//        // TODO: This logic is incorrect.  Many of the secondary sites
//        // do not have 2500 entries.  Only jboss.org does.
//        
//        logger.info("Parsing metrics from " + startDate + "...");
//        filescan:
//        	while(scanner.hasNextLine() && lineNum < END_OF_URI_METRICS_LINENUM )
//        	{
//        		while(scanner.hasNext()){
//        			WebMetric metric = new WebMetric();
//        			metric.setDate(startDate);
//        			String url = scanner.next();
//        			metric.setPage(url);
//
//        			try {
//        				metric.setPageViews(Resources.stripQuotes(scanner.next()));
//        				metric.setUniquePageViews(Resources.stripQuotes(scanner.next()));
//        				scanner.next(); // omit average time on page for now.
//        				metric.setEntrances(Resources.stripQuotes(scanner.next()));
//        				metric.setBounceRate(Resources.parsePercentage(scanner.next())); 
//        				metric.setPercentExit(Resources.parsePercentage(scanner.next()));
//        				lineNum++;
//        				scanner.nextLine(); //omit page value for now
//        				logger.fine("WebMetric = " + metric.toString());
//        				addOrUpdateMetric(metrics, metric);
//        			} 
//        			catch (java.util.NoSuchElementException nse){
//        				logger.warning("Scanner shit the bed at line: " + lineNum + "Stopping scan of file.");
//        				break filescan;
//        			}
//        			catch (NumberFormatException nfe)
//        			{
//        				logger.warning( "Issue with " + metric.getPage() + " in " + metric.getDate() 
//        						+ " at line " +  lineNum + ".  Stopping Scan");
//        				break filescan;
//        			}            
//        		}  // End of Line Scan
//        	} // End of File Scan
// 
//        //TODO:  Add summarized page-views that start on line 2511
//        scanner.close();
//        
//        logger.info("Saving metrics from " + startDate + " recording " + metrics.size() + " metrics");
//        saveMetrics(metrics);
//    	    
//        return metrics.values();
//    }
//    
//    private void saveMetrics(Map<String,WebMetric> metrics){
//
//    	assert metrics != null : "metrics must be specified";
//    	
//    	for (String key : metrics.keySet()){
//        	WebMetric metric = metrics.get(key);
//        	em.persist(metric);
//        }
//    	em.flush();
//    }
//    
//
//    /**
//     * There are many duplicates from jboss.org.  This method checks existing entries,
//     * and confirms there is only one.  Should a duplicate entry exist, the method
//     * sums the visits, entrances, unique pageviews and exits.  
//     * TODO:  Still need to do the moving averages correctly.
//     * TODO:  Parse the average time on page into seconds, then add to the metric
//     * TODO:  Associate project directly by the path name when possible.
//     * TODO:  Possibly make this JDG/ISPN based?
//     * @param metrics
//     * @param metric
//     */
//    private static void addOrUpdateMetric(Map<String, WebMetric> metrics, WebMetric metric)
//    {
//    	assert metric != null : "metric must be specified.";
//    	assert metrics != null : "metrics must be specified";
//    	
//    	String page = metric.getPage();
//    	
//    	//TODO: These rules would be way better in a drools spreadsheet.
//    	page = page.contains("?") ? StringUtils.substringBeforeLast(page, "?"):page;
//    	page = StringUtils.endsWith(page, "download.html")?StringUtils.substringBeforeLast(page, ".html"):page;
//    	page = StringUtils.endsWith(page, "index.html")?StringUtils.substringBeforeLast(page, "index.html"):page;
//    	page = StringUtils.endsWith(page,"tools.html")?StringUtils.substringBeforeLast(page,".html"):page;
//    	page = StringUtils.endsWith(page,".html")?StringUtils.substringBeforeLast(page,".html"):page;
//
//    	if (page.equalsIgnoreCase("/jbossorg-downloads/JBoss-6.0.0.Final") 
//    			|| page.equalsIgnoreCase("/jbossorg-downloads/JBoss-5.1.0.GA"))
//    		page = "/jbossas/downloads";
//    	if ( StringUtils.startsWith(page, "/tools/download/"))
//    		page = "/tools/download/";
//    	
//    	// Only URL that should have a trailing '/' is the root.
//    	if (StringUtils.endsWith(page, "/") && page.length() > 1){
//    		page = (StringUtils.substringBeforeLast(page, "/"));
//    	}
//    	
//    	if (metrics.containsKey(page)){
//
//    		// Add the metrics together
//    		WebMetric existingMetric = metrics.get(page);
//    		existingMetric.addMetrics(metric);
//    	} else {
//    		metric.setPage(page);
//    		metrics.put(page, metric);
//    	}    	
//    }    
    
}





