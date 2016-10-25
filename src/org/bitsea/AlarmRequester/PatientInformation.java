package org.bitsea.AlarmRequester;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.bitsea.AlarmRequester.CassandraConnector;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;


@Component
public class PatientInformation {

	Session session;
	
	private void connectSession() {
		String ipAddress = (System.getProperty("DBIP")!=null) ? System.getProperty("DBIP") : "127.0.0.1";
		int port = (System.getProperty("DBPORT")!=null) ? Integer.parseInt(System.getProperty("DBPORT")) : 9042;
		String keyspace = (System.getProperty("DBNAME")!=null) ? System.getProperty("DBNAME") : "message_database";

		final CassandraConnector client = new CassandraConnector();
		client.connect(ipAddress, port, keyspace);
		session = client.getSession();			
	}
	
	public String alarms(int id) {
		if (session == null) {connectSession();}
		
		Statement alarmsForPatient = QueryBuilder.select().column("reason")
									.column("severness_counter").column("tstamp")
									.from("alarm_information").where(QueryBuilder.eq("PatID", id));
		ResultSet result = session.execute(alarmsForPatient);
		String answer = getAlarmData(result);
		return answer;
	}
	
	
	private String getAlarmData(ResultSet result) {
		String answer = "";
		for (Row row : result) {
			Map<String, String> reasons = row.getMap("reason", String.class, String.class);
			Map<String, Integer> severness = row.getMap("severness_counter", String.class, Integer.class);
			Date date = new Date(Long.parseLong(row.getString("tstamp")));
			answer += "On " + date + " there were ";
			for (Entry<String, String> e : reasons.entrySet()) {
				answer += e.getKey() + " - " + e.getValue() + ",\n\t";
			}
			answer += " which yields overall ";
			for (Entry<String, Integer> e: severness.entrySet()) {
				answer += e.getValue() + "-times a " + e.getKey() + "alarm,";
			}
			answer += "\n";
		}
		return answer;
	}
	
	public String alarmsAfterTimestamp(int id, String tmstmp) {
		if (session == null) {connectSession();}
		
		Statement alarmsAfter = QueryBuilder.select().column("reason")
				.column("severness_counter").column("tstamp")
				.from("alarm_information").allowFiltering().where(QueryBuilder.eq("PatID", id)).and(QueryBuilder.gt("tstamp", tmstmp));
		ResultSet results = session.execute(alarmsAfter);
		String works = getAlarmData(results);
		return works;
	}
	
	public String alarmsBeforeTimestamp(int id, String tmstmp) {
		return null;
	}
}
