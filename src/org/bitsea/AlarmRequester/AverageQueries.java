package org.bitsea.AlarmRequester;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;

@Component
public class AverageQueries {

	static Session session = connectSession();
	
	private static Session connectSession() {
		String ipAddress = (System.getProperty("DBIP")!=null) ? System.getProperty("DBIP") : "127.0.0.1";
		int port = (System.getProperty("DBPORT")!=null) ? Integer.parseInt(System.getProperty("DBPORT")) : 9042;
		String keyspace = (System.getProperty("DBNAME")!=null) ? System.getProperty("DBNAME") : "message_database";

		final CassandraConnector client = new CassandraConnector();
		client.connect(ipAddress, port, keyspace);
		session = client.getSession();
		return session;
	}
	
	
	private static double calculateMean(List<Integer> ls) {
		Integer sum = 0;
		for (Integer item : ls) {
			sum += item;
		}
		if (sum > 0) {
			return sum.doubleValue() / ls.size();
		}
		return 0.0;
	}
	
	private static double calculateDeviation(List<Integer> ls, Double mean) {
		if (mean == null) {mean = calculateMean(ls);}
		double sum = 0.0;
		for (Integer item : ls) {
			sum += Math.pow(Math.abs(item - mean), 2);
		}
		if (sum > 0.0) {
			return Math.sqrt(sum / ls.size());
		}
		return 0.0;
	}

	/*
	 * average number of alarms
	 * patient: per message
	 * station: per patient
	 * optional argument: time, after this point in time check
	 * optional argument: severness, only alarms of a certain severness are of interest
	 * 
	 * 
	 */
	public static String alarmsAverage(String patOrStat, int id, Object o1, Object o2) {
		long time = -1L;
		String severness = "";

		if (o1 != null) {
			if (o1 instanceof Long) {
				time = (Long) o1;
			} else if (o1 instanceof String) {
				severness = (String) o1;
			}
		}
		if (o2 != null) {
			if (o2 instanceof String) {
				severness = (String) o2;
			} else if (o2 instanceof Long) {
				time = (Long) o2;
			}
		}
		
		Where stmt;
		if (patOrStat.equalsIgnoreCase("station")) {
			stmt = QueryBuilder.select().column("patid")
					.column("time").from("patient_bed_station")
					.where(QueryBuilder.eq("station", id));
			ResultSet rs = session.execute(stmt);
			List<Integer> patientIds = new LinkedList<Integer>();
			for (Row row : rs) {
				if (row.getLong("time") > time) {
					patientIds.add(row.getInt("patid"));
				}
			}
			stmt = QueryBuilder.select().all().from("alarm_information")
					.where(QueryBuilder.in("patid", patientIds));
		} else {
			stmt = QueryBuilder.select().all().from("alarm_information")
					.where(QueryBuilder.eq("patid", id));
		}

		if (time != -1) {
			stmt.and(QueryBuilder.gt("sendTime", time));
		}
		// supposed to contain all results either from a single patient 
		// or all patients from station X after time Y 
		ResultSet queryResult = session.execute(stmt);
		
//		// now: this is just about color-code filtering and averaging
//		// either averaging all alarms or averaging each group as is?
//		// if done as is -> simpler, otherwise filtering instead of dropping in the end
		Map<String, List<Integer>> result = new HashMap<String, List<Integer>>();
		
		for (Row row : queryResult) {
			Map<String, Integer> tmp = row.getMap("severness_counter", String.class, Integer.class);
			for (Entry<String, Integer> e : tmp.entrySet()) {
				List<Integer> tp = result.getOrDefault(e.getKey(), new LinkedList<>());
				tp.add(e.getValue());
				result.put(e.getKey(), tp);
			}
		}
		
//		// calculate mean, std for each alarm or for the special one
		String finalResult = "<table border=2><tr><td>Type</td><td>Mean +/- StD</td><td>Max</td></tr>";
		if (!severness.isEmpty()) {
			List<Integer> ls = result.getOrDefault(severness, new LinkedList<Integer>());
			finalResult = MessageFormat.format("<tr><td>{0}</td><td>{1} +/- {2}</td><td>{3}</td></tr>", severness,
					calculateMean(ls), calculateDeviation(ls, null),
					Collections.max(ls));
		} else {
			for (Entry<String, List<Integer>> e: result.entrySet()) {
				finalResult += MessageFormat.format("<tr><td>{0}</td><td>{1} +/- {2}</td><td>{3}</td></tr>", e.getKey(), 
						calculateMean(e.getValue()), 
						calculateDeviation(e.getValue(), null),
						Collections.max(e.getValue()));
			}
		}
		return finalResult + "</table>";
	}
}
