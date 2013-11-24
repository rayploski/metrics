package org.jboss.bigcommotion.services;

import org.apache.commons.lang.StringUtils;
import org.jboss.bigcommotion.model.WebMetric;
import org.jboss.bigcommotion.util.Resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Imports a CSV formatted Google Analytics page-view report based on standard defaults.
 */
@Stateless
public class GoogleAnalyticsImportService {
	
   private static final String DATE_FORMAT = "YYYYMMdd";
   private static final String REGEX_COMMAS_AND_QUOTES = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
   private static final String PERSISTENCE_CONTEXT_NAME = "metrics-big-commotion";
   private static final int END_OF_URI_METRICS_LINENUM = 2506;

   
   @PersistenceContext(unitName = PERSISTENCE_CONTEXT_NAME)
   private EntityManager em;
    
   @Inject
   private transient Logger logger;

   /**
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
	   Date startDate = getStartDate(filePath.getName());
       
       if (!filePath.isDirectory()){    	   // Process a single file
    	   try {
    		   parseFile(filePath, startDate);
        	} catch (Exception e){
        		logger.log(Level.SEVERE, "Error parsing file " + filePath.getName(), e);
        	}
        } else {        	// Process a directory
            File[] files = filePath.listFiles();
            for (int i = 0; i < files.length; i++){

            	//omit Mac specific .DS_Store file
        		if (StringUtils.endsWith(files[i].getName(), ".DS_Store")){
        			continue;
        		}
        		
        		try {
        			parseFile(files[i], startDate);
        		} catch (Exception e){
        			logger.log(Level.SEVERE, "Error parsing file: " + files[i].getName(), e);
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

        String dateRange = StringUtils.substringAfterLast(fileName, " ");
        String startDateStr = StringUtils.substringBefore(dateRange, "-");
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        Date startDate = null;

        try {
        	logger.log(Level.FINE, "********** dateRange = " + dateRange);
            logger.log(Level.FINE, "********** startDateStr = " + startDateStr);
            startDate = sdf.parse(startDateStr);
        } catch (ParseException pe){
        	
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


    /**
     * 
     * @param file
     * @param startDate
     * @return
     * @throws Exception
     */
    private List<WebMetric> parseFile(File file, Date startDate) throws Exception{
        
    	Map<String, WebMetric> metrics = new HashMap<String, WebMetric>();

        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedFileReader = new BufferedReader(fileReader);
    	Scanner scanner = new Scanner(bufferedFileReader);
		long lineNum = 0;
		
        // Skip the metadata for now.
        while (lineNum < 7){
            scanner.nextLine();
        	lineNum++;
        }

        scanner.useDelimiter(REGEX_COMMAS_AND_QUOTES);
        
        // Scan each line.  
        // TODO: This logic is incorrect.  Many of the secondary sites
        // do not have 2500 entries.  Only jboss.org does.
        while(scanner.hasNextLine() && lineNum < END_OF_URI_METRICS_LINENUM )
        {
            while(scanner.hasNext()){
            	WebMetric metric = new WebMetric();
                metric.setDate(startDate);
                String url = scanner.next();
                metric.setPage(url);
                
            	try {
            		metric.setPageViews(Resources.stripQuotes(scanner.next()));
                    metric.setUniquePageViews(Resources.stripQuotes(scanner.next()));
                    scanner.next(); // omit average time on page for now.
                    metric.setEntrances(Resources.stripQuotes(scanner.next()));
                    metric.setBounceRate(Resources.parsePercentage(scanner.next())); 
                    metric.setPercentExit(Resources.parsePercentage(scanner.next()));
                    lineNum++;
                    scanner.nextLine(); //omit page value for now
                    logger.log(Level.FINER, metric.toString());
                    addOrUpdateMetric(metrics, metric);
            	} 
            	catch (java.util.NoSuchElementException nse){
            		logger.log(Level.SEVERE,"Scanner shit the bed after line: " + lineNum);
            		nse.printStackTrace();	
            	}
            	catch (NumberFormatException nfe)
            	{
            		logger.log(Level.WARNING, "Issue with " + metric.getPage() + " in " + metric.getDate() + ": ");
            		nfe.printStackTrace();
            	}
            
            }
            
            // persist all records
            for (String key : metrics.keySet()){
            	WebMetric metric = metrics.get(key);
            	metric = em.merge(metric);
            }
        } // End of File Scan
        
            
        //TODO:  Add summarized page-views that start on line 2511
        scanner.close();
    	    
        return null;
    }
    

    /**
     * There are many duplicates from jboss.org.  This method checks existing entries,
     * and confirms there is only one.  Should a duplicate entry exist, the method
     * sums the visits, entrances, unique pageviews and exits.  
     * TODO:  Still need to do the moving averages correctly.
     * TODO:  Possibly make this JDG/ISPN based?
     * @param metrics
     * @param metric
     */
    private static void addOrUpdateMetric(Map<String, WebMetric> metrics, WebMetric metric)
    {
    	String page = metric.getPage();
    	
    	// Only URL that should have a trailing '/' is the root.
    	if (StringUtils.contains(page, "/") && page.length() > 1){
    		page = (StringUtils.substringBeforeLast(page, "/"));
    	}
    	
    	page = StringUtils.substringBeforeLast(page, ".html");
    	
    	if (metrics.containsKey(page)){

    		// Add the metrics together
    		WebMetric existingMetric = metrics.get(page);
    		existingMetric.addMetrics(metric);
    	} else {
    		metric.setPage(page);
    		metrics.put(page, metric);
    	}    	
    }    
    
}





