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
			.get("/alarms/{filteredBy}")
			.to("bean:PatientInformation?method=alarms(${header.id}, ${header.filteredBy})")
			.get("/therapychange")
			.to("bean:PatientInformation?method=changedTherapy(${header.id})")
			.get("/simultaneousAlarms")
			.to("bean:PatientInformation?method=simultaneousAlarms(${header.id})")
			.get("/simultaneousAlarms/{time}/{key}")
			.to("bean:PatientInformation?method=simultaneousAlarms(${header.id}, ${header.time}, ${header.key})")
			.get("/simultaneousAlarms/{time}")
			.to("bean:PatientInformation?method=simultaneousAlarms(${header.id}, ${header.time})")
//			.get("/timeToAlarmAck")
//			
			.get("/timeUntilNormalized")
			.to("bean:PatientInformation?method=timeBetweenNormal(${header.id})")
			.get("/timeUntilNormalized/around/{dt}")
			.to("bean:PatientInformation?method=timeBetweenNormal(${header.id}, null, ${header.dt})")
			.get("/timeUntilNormalized/{alarmtype}")
			.to("bean:PatientInformation?method=timeBetweenNormal(${header.id}, ${header.alarmtype})")
			.get("/timeUntilNormalized/{alarmtype}/{timepoint}")
			.to("bean:PatientInformation?method=timeBetweenNormal(${header.id}, ${header.alarmtype}, ${header.timepoint})")
//			.get("/implausibleAlarms")
//			
			.get("/changeAfterAlarm")
			.to("bean:PatientInformation?method=changeAfterAlarm(${header.id})")
			.get("/changeAfterAlarm/{ts}")
			.to("bean:PatientInformation?method=changeAfterAlarm(${header.id}, ${header.ts})")
			;
			
	}

}
