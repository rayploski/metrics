package org.jboss.bigcommotion.rest;

import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.jboss.bigcommotion.services.GoogleAnalyticsImportSingleton;



@Stateless
@Path("/import-metrics")
public class MetricImportEndpoint {


    @Inject GoogleAnalyticsImportSingleton importer;
    
    @GET
    public Response importFiles (@QueryParam("siteName")String siteName, @QueryParam("importDir")String importDir){

        importer.go(siteName, importDir);
        return Response.ok().build();
    }

}
