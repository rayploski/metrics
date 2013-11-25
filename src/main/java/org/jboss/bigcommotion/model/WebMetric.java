package org.jboss.bigcommotion.model;

import javax.persistence.Entity;
import java.io.Serializable;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Column;
import javax.persistence.Version;
import java.lang.Override;
import java.util.Date;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
public class WebMetric implements Serializable
{

   @Id
   @GeneratedValue(strategy = GenerationType.AUTO)
   @Column(name = "id", updatable = false, nullable = false)
   private Long id = null;
   @Version
   @Column(name = "version")
   private int version = 0;

   @Temporal(TemporalType.DATE)
   private Date date;

   @Column
   private String site;

   @Column
   private String page;

   @Column
   private long pageViews;

   @Column
   private long uniquePageViews;

   @Column
   private int averageTimeOnPage;

   @Column
   private long entrances;

   @Column
   private Float bounceRate;

   @Column
   private Float percentExit;
   
   @Column
   private String fileName;
   
   @Column
   private String project;
   

   public String getProject() {
	return project;
}

public void setProject(String project) {
	this.project = project;
}

public String getFileName() {
	return fileName;
}

public void setFileName(String fileName) {
	this.fileName = fileName;
}

public Long getId()
   {
      return this.id;
   }

   public void setId(final Long id)
   {
      this.id = id;
   }

   public int getVersion()
   {
      return this.version;
   }

   public void setVersion(final int version)
   {
      this.version = version;
   }

   @Override
   public boolean equals(Object that)
   {
      if (this == that)
      {
         return true;
      }
      if (that == null)
      {
         return false;
      }
      if (getClass() != that.getClass())
      {
         return false;
      }
      if (id != null)
      {
         return id.equals(((WebMetric) that).id);
      }
      return super.equals(that);
   }

   @Override
   public int hashCode()
   {
      if (id != null)
      {
         return id.hashCode();
      }
      return super.hashCode();
   }

   public Date getDate()
   {
      return this.date;
   }

   public void setDate(final Date date)
   {
      this.date = date;
   }

   public String getSite()
   {
      return this.site;
   }

   public void setSite(final String site)
   {
      this.site = site;
   }

   public String getPage()
   {
      return this.page;
   }

   public void setPage(final String page)
   {
      this.page = page;
   }

   public long getPageViews()
   {
      return this.pageViews;
   }

   public void setPageViews(final long pageViews)
   {
      this.pageViews = pageViews;
   }

   public long getUniquePageViews()
   {
      return this.uniquePageViews;
   }

   public void setUniquePageViews(final long uniquePageViews)
   {
      this.uniquePageViews = uniquePageViews;
   }

   public int getAverageTimeOnPage()
   {
      return this.averageTimeOnPage;
   }

   public void setAverageTimeOnPage(final int averageTimeOnPage)
   {
      this.averageTimeOnPage = averageTimeOnPage;
   }

   public long getEntrances()
   {
      return this.entrances;
   }

   public void setEntrances(final long entrances)
   {
      this.entrances = entrances;
   }

   public Float getBounceRate()
   {
      return this.bounceRate;
   }

   public void setBounceRate(final Float bounceRate)
   {
      this.bounceRate = bounceRate;
   }

   public Float getPercentExit()
   {
      return this.percentExit;
   }

   public void setPercentExit(final Float percentExit)
   {
      this.percentExit = percentExit;
   }
   
   
   public void addMetrics(WebMetric metric){
	   
	   double pctBounce = ((this.getPageViews() * this.getBounceRate().doubleValue() * .01) 
			   + (metric.getPageViews() * metric.getBounceRate().doubleValue() * 0.01)) 
			   / ((this.getPageViews() + metric.getPageViews()) * 100); 

	   double pctExit = ((this.getPageViews() * this.getPercentExit().doubleValue() * .01) 
			   + (metric.getPageViews() * metric.getPercentExit().doubleValue() * 0.01)) 
			   / ((this.getPageViews() + metric.getPageViews()) * 100); 
	  
	   this.setBounceRate(new Float(pctBounce));
	   this.setPercentExit(new Float(pctExit));
	   this.setEntrances(this.getEntrances() + metric.getEntrances());
	   this.setPageViews(this.getPageViews() + metric.getPageViews());
	   this.setUniquePageViews(this.getUniquePageViews() + metric.getUniquePageViews());
	   
	   //	   this.setAverageTimeOnPage(averageTimeOnPage);
   }

   @Override
   public String toString()
   {
      String result = getClass().getSimpleName() + " ";
      if (site != null && !site.trim().isEmpty())
         result += "site: " + site;
      if (page != null && !page.trim().isEmpty())
         result += ", page: " + page;
      result += ", pageViews: " + pageViews;
      result += ", uniquePageViews: " + uniquePageViews;
      result += ", averageTimeOnPage: " + averageTimeOnPage;
      result += ", entrances: " + entrances;
      if (bounceRate != null)
         result += ", bounceRate: " + bounceRate;
      if (percentExit != null)
         result += ", percentExit: " + percentExit;
      return result;
   }
}