package org.bitsea.AlarmRequester;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bitsea.AlarmRequester.CassandraConnector;
import org.springframework.stereotype.Component;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;

//import org.bitsea.AlarmRequester.utils.Pair;

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
									.column("severness_counter").column("receivedTime")
									.column("sendTime").from("alarm_information")
									.where(QueryBuilder.eq("PatID", id));
		ResultSet result = session.execute(alarmsForPatient);
		String answer = getAlarmData(result);
		return answer;
	}
	
	
	public String alarms(int id, String filter) {
		if (session == null) {connectSession();}
		
		Statement alarmsForPatient = QueryBuilder.select().column("reason")
									.column("severness_counter").column("receivedTime")
									.column("sendTime").from("alarm_information")
									.where(QueryBuilder.eq("PatID", id));
		ResultSet result = session.execute(alarmsForPatient);
		String answer = getFilteredData(result, filter);
		return answer;
		
	}
	
	
	private String getAlarmData(ResultSet result) {
		String answer = "";
		for (Row row : result) {
			Set<String> reasons = row.getSet("reason", String.class);
			Map<String, Integer> severness = row.getMap("severness_counter", String.class, Integer.class);
			Date date = new Date(row.getLong("sendTime"));
			answer += "On " + date + " there were ";
			Iterator<String> reasonIter = reasons.iterator();
			while (reasonIter.hasNext()) {
				answer += reasonIter.next() + ",\n\t";
			}
			answer += " which yields overall ";
			for (Entry<String, Integer> e: severness.entrySet()) {
				answer += e.getValue() + "-times a " + e.getKey() + "alarm,";
			}
			answer += "\n";
		}
		return answer;
	}
	
	
	private String getFilteredData(ResultSet result, String filterBy) {
		String answer = "";
		for (Row row : result) {
			Set<String> reasons = row.getSet("reason", String.class);
			Iterator<String> rIter = reasons.iterator();
			while (rIter.hasNext()) {
				String r = rIter.next();
				if (r.contains(filterBy)) {
					Date date = new Date(row.getLong("sendTime"));
					answer += "On " + date + " there were " + r + "\n";
				}
			}
		}
		return answer;
	}
	
	
	public String alarmsAfterTimestamp(int id, long tmstmp) {
		if (session == null) {connectSession();}
		
		Statement alarmsAfter = QueryBuilder.select().column("reason")
				.column("severness_counter").column("sendTime")
				.from("alarm_information").allowFiltering()
				.where(QueryBuilder.eq("PatID", id))
				.and(QueryBuilder.gt("sendTime", tmstmp));
		ResultSet results = session.execute(alarmsAfter);
		String works = getAlarmData(results);
		return works;
	}
	
	
	public String alarmsBeforeTimestamp(int id, long tmstmp) {
		if (session==null) {connectSession();}
		Statement alarmsBefore = QueryBuilder.select().column("reason")
				.column("severness_counter").column("sendTime")
				.from("alarm_information").allowFiltering().where(QueryBuilder.eq("PatID", id))
				.and(QueryBuilder.lt("sendTime", tmstmp));
		ResultSet results = session.execute(alarmsBefore);
		String answer = getAlarmData(results);
		return answer;
	}
	
	
	private String extractParameters(Row x) {
		String parameterString = "";
		Map<String, TupleValue> paras;
		paras = x.getMap("parameters", String.class, TupleValue.class);
		for (Entry<String, TupleValue> e: paras.entrySet()) {
			parameterString += e.getKey() + " - " + e.getValue().toString() + ", ";
		}
		
		return parameterString;
	}
	
	/*
	 * shows whether the alarm borders of a patient has been changed;
	 * if it was changed, old and new values are compared
	 * mis-named operation, do not know how therapy change could be registered
	 */
	public String changedTherapy(int patientID) {
		if (session==null) {connectSession();}
		Statement getAllStandardValues = QueryBuilder.select().column("parameters")
				.column("sendTime").from("patient_standard_values").where(
						QueryBuilder.eq("PatID", patientID));
		ResultSet results = session.execute(getAllStandardValues);
		String answer = "";
		String current = "";
		if (!results.isExhausted()) {
			Row row = results.one();
			current += extractParameters(row);
			current += "\n";
		}
		
		if (!results.isExhausted()) {
			while (!results.isExhausted()) {
				Row row = results.one();
				answer += extractParameters(row);
				answer += "\n";
			}
		}
		current = "Current parameters: " + current;
		if (!answer.isEmpty()){
			current = current + "\nOld Parameters: " + answer;
		}
		return current;
	}

	
	public String simultaneousAlarms(int patientID, Long when, String before) {
		when = when != null ? when : null;
		before = before != null ? before : "false";
		
		if (session==null) {connectSession();}
		
		Where stmt = QueryBuilder.select().column("severness_counter")
				.column("reason").column("sendTime")
				.from("alarm_Information").allowFiltering()
				.where(QueryBuilder.eq("PatID", patientID)); 
		if (when != null) {
			if (before.equalsIgnoreCase("before")) {
				stmt.and(QueryBuilder.lt("sendTime", when));
			} else {
				stmt.and(QueryBuilder.gt("sendTime", when));
			}
		}
		String answer = "";
		ResultSet results = session.execute(stmt);
		for (Row row : results) {
			Map<String, Integer> severness = row.getMap("severness_counter", String.class, Integer.class);
			int numberOfAlarms = severness.values().stream().reduce((a,b) -> a+b).get();
			if (numberOfAlarms > 1) {
				Date date = new Date(row.getLong("sendTime"));
				Set<String> alarms = row.getSet("reason", String.class);
				answer += "There have been several alarms on " + date + ": ";
				for (String e: alarms) {
					answer += e + ", ";
				}
				answer += "\n";
			}
		}
		return answer;
	}

	
	private long closest(long of, List<Long> stamps) {
		long min = Long.MIN_VALUE;
		long closest = of;
		for (long v : stamps) {
			long diff = Math.abs(v-of);
			if (diff < min) {
				min = diff;
				closest = v;
			}
		}
		return closest;
	}
	
	
	/*
	 * if timepoint and alarm are given, check the duration of an alarm of this kind around this timepoint
	 * if only timepoint is given, check the duration of any alarm around this timepoint
	 * if only alarm is given, check the duration of the last alarm of this type
	 * if nothing is given, check the duration of the last alarm
	 */
	public long timeBetweenNormal(int patientID, String alarm, Long timepoint) {
		timepoint = timepoint != null ? timepoint : null;
		alarm = alarm != null ? alarm : null;
		if (session==null) {connectSession();}
		
		Where stmt;
		stmt = QueryBuilder.select().column("sendTime").column("reason")
				.from("alarm_information").allowFiltering()
				.where(QueryBuilder.eq("PatID", patientID));
		
		ResultSet results = session.execute(stmt);
		Iterator<Row> it = results.iterator();
		List<Long> tstampList = new ArrayList<Long>();
		while (it.hasNext()) {
			Row row = it.next();
			Set<String> ss = row.getSet("reason", String.class);
			if (alarm != null) {
				Iterator<String> rIter = ss.iterator();
				while (rIter.hasNext()) {
					if (rIter.next().contains(alarm)) {
						tstampList.add(row.getLong("sendTime"));						
					}
				}
			} else {
				tstampList.add(row.getLong("sendTime"));
			}
		}

		long interestingPoint;
		if (tstampList.size() == 0) {
			return -1L;
		}
		
		if (timepoint != null) {
			interestingPoint = timepoint;
		} else {
			interestingPoint = Collections.max(tstampList);
		}

		Collections.sort(tstampList, (a,b)->a.compareTo(b));
		boolean[] differences = new boolean[tstampList.size()];

		for (int i=0; i<differences.length-1; i++) {
			differences[i] = (tstampList.get(i+1) - tstampList.get(i)) < 5000;
		}
		
		int i, j;
		int tmp = timepoint != null ? tstampList.indexOf(closest(interestingPoint, tstampList)) : 0;
				
		for (i=tmp;i>0;i--) {
			if (!differences[i]) {
				break;
			}
		}
		
		for (j=tstampList.size()-1; j >= tmp; j-- ) {
			if (differences[j]) {
				break;
			}
		}
		
		if (j == differences.length-1) {
			if (tstampList.get(tstampList.size()) - tstampList.get(tstampList.size()-1) < 5000) {
				j = tstampList.size();
			}
		}
		long result = tstampList.get(j) - tstampList.get(i);
		return result;
	}


	public boolean changeAfterAlarm(int patID, Long alarm) {
		alarm = alarm != null ? alarm : System.currentTimeMillis(); 
		if (session==null) {connectSession();}
		
		// select last alarm timestamp
		Statement alarmStamps = QueryBuilder.select("sendTime").from("alarm_information")
				.where(QueryBuilder.eq("PatID", patID));
		ResultSet rs = session.execute(alarmStamps);
		List<Long> stamps = new ArrayList<Long>();
		for (Row row : rs) {
			stamps.add(row.getLong("sendTime"));
		}
		
		long alarmT = closest(alarm, stamps);
		
		// select last new patient standard values timestamp (that which is true)
		Statement newestValues = QueryBuilder.select("sendTime").from("patient_standard_values")
				.allowFiltering().where(QueryBuilder.eq("current", true))
				.and(QueryBuilder.eq("PatID", patID));
		rs = session.execute(newestValues);
		Row row = rs.one();
		long pupdate = -1L;
		if (row != null) {
			pupdate = row.getLong("sendTime");
		}
		
		
		// if tstamp2 > tstamp1 true else false
		if (pupdate > -1L && (pupdate - alarmT)>0) {return true;}
		return false;
	}

}
