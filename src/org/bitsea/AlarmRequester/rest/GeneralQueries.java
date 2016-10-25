package org.bitsea.AlarmRequester.rest;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestParamType;


public class GeneralQueries extends RouteBuilder {

	@Override
	public void configure() throws Exception {
		restConfiguration().component("jetty")
		.bindingMode(RestBindingMode.json)
		.dataFormatProperty("prettyPrint", "true")
		.port(8080);
		
		
		rest("/patient/{id}")
			.consumes("text/plain").produces("text/plain")
			.get("/alarms") //.outType(classType)  // add some nice out type?
			.to("bean:PatientInformation?method=alarms(${header.id})")
			.get("/alarms/after/{dt}")
			.to("bean:PatientInformation?method=alarmsAfterTimestamp(${header.id}, ${header.dt})")
			.get("/alarms/before/{dt}")
			.to("bean:PatientInformation?method=alarmsBeforeTimestamp(${header.id}, ${header.dt})")
//			.get("/therapychange")
//			
//			.get("/simultaneousAlarms")
//			
//			.get("/timeToAlarmAck")
//			
//			.get("/numberUntilNormalized")
//			
//			.get("/implausibleAlarms")
//			
//			.get("/changeAfterAlarm")
			;
			
	}

}
