package org.jboss.bigcommotion.services;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.jboss.bigcommotion.model.WebMetric;
import org.jboss.bigcommotion.util.Resources;

@MessageDriven(name="PageviewParserMDB", activationConfig = {
		@ActivationConfigProperty(propertyName="destinationType", propertyValue="queue"),
		@ActivationConfigProperty(propertyName="destination", propertyValue=Resources.PAGEVIEW_QUEUE),
		@ActivationConfigProperty(propertyName="acknowledgeMode", propertyValue="auto-acknowledge"),
		@ActivationConfigProperty(propertyName="maxSession",propertyValue="10")
		})
public class GoogleAnalyticsPageviewImporterMDB implements MessageListener{
	
	@PersistenceContext(unitName = Resources.PERSISTENCE_CONTEXT_NAME)
	private EntityManager em;

	@Inject
	private transient Logger logger;

	@Inject private AnalyticsPageViewParser parser;
	
	@Override
	public void onMessage(Message rcvMessage) {
		ObjectMessage msg = null;
		
		try {
			if (rcvMessage instanceof ObjectMessage)
			{
				msg = (ObjectMessage)rcvMessage;
				if (msg.getObject() instanceof WebMetric){
					WebMetric m = (WebMetric)msg.getObject();
					logger.info("Received message from queue " +  m.toString());
					parser.parseFile(m.getSite(), new File(m.getFileName()), m.getDate());
				}
			}			
		} catch (JMSException jmsE){
			logger.log(Level.SEVERE, "Issue processing message. ", jmsE);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Issue processing file: " , e);
		}
	}
}
