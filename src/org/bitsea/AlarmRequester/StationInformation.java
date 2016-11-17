package org.bitsea.AlarmRequester;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;


@Component
public class StationInformation {
	
	Session session;
	
	private void connectSession() {
		String ipAddress = (System.getProperty("DBIP")!=null) ? System.getProperty("DBIP") : "127.0.0.1";
		int port = (System.getProperty("DBPORT")!=null) ? Integer.parseInt(System.getProperty("DBPORT")) : 9042;
		String keyspace = (System.getProperty("DBNAME")!=null) ? System.getProperty("DBNAME") : "message_database";

		final CassandraConnector client = new CassandraConnector();
		client.connect(ipAddress, port, keyspace);
		session = client.getSession();	
	}
	
	/*
	 * get all alarms from station
	 */
	public String alarms(int stationID, String timePeriod, String alarmType) {
		timePeriod = timePeriod != null ? timePeriod : "None";
		alarmType = alarmType != null ? alarmType : null;
		if (session==null) {connectSession();}
		
		long timeChange = -1;
		
		if (timePeriod.equalsIgnoreCase("hour")) {
			timeChange = 60*60*1000;
		} else if (timePeriod.equalsIgnoreCase("day")) {
			timeChange = 24*60*60*1000;
		} else if (timePeriod.equalsIgnoreCase("week")) {
			timeChange = 7*24*60*60*1000;
		} else if (timePeriod.equalsIgnoreCase("year")) {
			timeChange = 365*7*24*60*60*1000;			
		}
		
		Statement timeQuery;
		
		if (timeChange > 0) {
			timeQuery = QueryBuilder.select().from("alarm_information").allowFiltering().where(QueryBuilder.lt("tstamp", System.currentTimeMillis() - timeChange));
		} else {
			timeQuery = QueryBuilder.select().from("alarm_information");
		}
		
		ResultSet results = session.execute(timeQuery);
		String answer = "";

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
		int nrAlarms = 0;
		for (Row row : results) {
			int oldNr = nrAlarms;
			int patID = row.getInt("patid");
			String msgctrlid = row.getString("msgctrlid");
			Set<String> reason = row.getSet("reason", String.class);
			Map<String, Integer> counter = row.getMap("severness_counter", String.class, Integer.class);
			Date dateUnformatted = new Date(row.getLong("tstamp"));
			String date = sdf.format(dateUnformatted);
			String line = "At time " + date.toString() + " patient " + patID + " had message " + msgctrlid + " delivered for the following reasons: ";
			Iterator<String> iter = reason.iterator();
			while (iter.hasNext()) {
				String alarmX = iter.next();
				if (alarmType != null) {
					if (alarmX.contains(alarmType) || alarmX.equalsIgnoreCase(alarmType)) {
						line += alarmX + ", ";
						nrAlarms += 1;
					}
				} else {
					nrAlarms += 1;
					line += alarmX + ", ";
				}
			}
			if (alarmType == null) {
				line += " with the following severenesses: ";
				for (Entry<String, Integer> e: counter.entrySet()) {
					line += e.getKey() + " appeared " + e.getValue() + " times,";
				}
			}
			if (nrAlarms - oldNr > 0) {
				answer += line + "\n";
			}
		}
		return "Overall, there have been " + nrAlarms + " Alarms:\n" + answer;
	}
	
	/*
	 * how many alarm border changes have been made on the station altogether
	 * after
	 */
	public String therapyChanges(int stationID, Long after) {
		after = after != null ? after : null;
		
		if (session==null) { connectSession(); }
		
		// get all patients of a station
		// get the number of entries per patient from the patient_standard_values
		// maybe even timed (have to integrate this in the psv-table)
		
		return "";
	}
}
