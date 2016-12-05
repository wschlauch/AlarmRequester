package org.bitsea.AlarmRequester.rest;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;


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
//			.get("/simultaneousAlarms")
//			.to("bean:PatientInformation?method=simultaneousAlarms(${header.id})")
//			.get("/simultaneousAlarms/{time}/{key}")
//			.to("bean:PatientInformation?method=simultaneousAlarms(${header.id}, ${header.time}, ${header.key})")
//			.get("/simultaneousAlarms/{time}")
//			.to("bean:PatientInformation?method=simultaneousAlarms(${header.id}, ${header.time})")
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
		
		// interesting information on stations should always include
		// number, mean, std, min, max
					
		rest("/station/{stationID}")
			.consumes("text/plain").produces("text/plain")
			.get("/alarms") //.outType(classType)  // add some nice out type?
			.to("bean:StationInformation?method=alarms(${header.stationID})")
			.get("/alarms/{time}")
			.to("bean:StationInformation?method=alarms(${header.stationID}, ${header.time})")
			.get("/alarms/type/{type}")
			.to("bean:StationInformation?method=alarms(${header.stationID}, null, ${header.type})")
		;
		
		rest("{who}/{id}")
			.consumes("text/plain").produces("text/html")
			.get("/averageAlarms")
			.to("bean:AverageQueries?method=alarmsAverage(${header.who}, ${header.id}, null, null)")
			.get("/averageAlarms/{code1}")
			.to("bean:AverageQueries?method=alarmsAverage(${header.who}, ${header.id}, ${header.code1}, null)")
			.get("/averageAlarms/{code1}/{code2}")
			.to("bean:AverageQueries?method=alarmsAverage(${header.who}, ${header.id}, ${header.code1}, ${header.code2})")
			.get("/simultaneousAlarms")
			.to("bean:AverageQueries?method=simultaneousAlarms(${header.who}, ${header.id}, null, null)")
			.get("/simultaneousAlarms/{time}")
			.to("bean:AverageQueries?method=simultaneousAlarms(${header.who}, ${header.id}, ${header.time}, null)")
			.get("/simultaneousAlarms/{time}/{span}")
			.to("bean:AverageQueries?method=simultaneousAlarms(${header.who}, ${header.id}, ${header.time}, ${header.span})")
			.get("/closeness")
			.to("bean:AverageQueries?method=closenessByTime(${header.who}, ${header.id}, null)")
			.get("/closeness/{alarmType}")
			.to("bean:AverageQueries?method=closenessByTime(${header.who}, ${header.id}, ${header.alarmType})")
			.get("/implausible")
			.to("bean:AverageQueries?method=implausibleAlarms(${header.who}, ${header.id})")
			.get("/therapy")
			.to("bean:AverageQueries?method=reactionToAlarm(${header.who}, ${header.id}, null, null)")
			.get("/therapy/{c1}")
			.to("bean:AverageQueries?method=reactionToAlarm(${header.who}, ${header.id}, ${header.c1}, null)")
			.get("/therapy/{c1}/{c2}")
			.to("bean:AverageQueries?method=reactionToAlarm(${header.who}, ${header.id}, ${header.c1}, ${header.c2})")
		;
	}

}
