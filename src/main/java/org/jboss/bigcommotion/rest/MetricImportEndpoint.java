package org.jboss.bigcommotion.rest;

import org.jboss.bigcommotion.services.GoogleAnalyticsImportService;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;


@Stateless
@Path("/import-metrics")
public class MetricImportEndpoint {


    @Inject GoogleAnalyticsImportService importer;
    
    @GET
    public Response importFiles (@QueryParam("siteName")String siteName, @QueryParam("importDir")String importDir){

        importer.go(siteName, importDir);
        return null;
    }

}
