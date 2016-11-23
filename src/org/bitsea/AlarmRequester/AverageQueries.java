package org.bitsea.AlarmRequester;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.bitsea.AlarmRequester.utils.AlarmDuration;
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

	
	private static List<Integer> getPatientsFromStation(int StationID, Long time) {
		time = time != null ? time : 0;
		Where stmt;
		stmt = QueryBuilder.select().column("patid")
				.column("time").from("patient_bed_station")
				.where(QueryBuilder.eq("station", StationID));
		ResultSet rs = session.execute(stmt);
		List<Integer> patientIds = new LinkedList<Integer>();
		for (Row row : rs) {
			if (row.getLong("time") > time) {
				patientIds.add(row.getInt("patid"));
			}
		}
		return patientIds;
	}
	/*
	 * average number of alarms
	 * patient: per message
	 * station: per patient
	 * optional argument: time, after this point in time check
	 * optional argument: severeness, only alarms of a certain severeness are of interest
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
			List<Integer> patientIds = getPatientsFromStation(id, time);
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

	/*
	 * get all simultaneous alarms in a certain time range around a time point
	 * time: optional parameter around that a timeframe is analyzed, standardvalue: now
	 * span: time +/- span, standardvalue: 100s
	 */
	public static String simultaneousAlarms(String patOrStat, int id, Long time, Long span) {
		// standard time difference being 100s
		span = span != null ? span : 100000;
		
		// if no time is given, take the current System-time
		if (time == null) {
			time = System.currentTimeMillis();
		}
		
		Where stmt;
				
		if (patOrStat.equalsIgnoreCase("patient")) {
			stmt = QueryBuilder.select().all().from("alarm_information").allowFiltering()
					.where(QueryBuilder.eq("patid", id)).and(QueryBuilder.gt("sendTime", time-span))
					.and(QueryBuilder.lt("sendTime", time+span));
		} else {
			List<Integer> patients = getPatientsFromStation(id, time);
			stmt = QueryBuilder.select().all().from("alarm_information").allowFiltering()
					.where(QueryBuilder.in("patid", patients)).and(QueryBuilder.gt("sendTime", time - span))
					.and(QueryBuilder.lt("sendTime", time + span));
		}
		
		ResultSet results = session.execute(stmt);
		String answer = "<table border=2><tr><td>Severness</td><td>Count</td></tr>";
		
		// add timestamps to the possibleTime set (?) 
		// results.forEach(r -> possibleTimes.add(r.getLong("sendTime")));
		
		// for each timestamp, get simultaneous alarms (i.e. sum over array per person, average over all of these)
		// do this with a mapping
		HashMap<Long, Integer> timeOccMapping = new HashMap<Long, Integer>();
		for (Row r : results) {
			long thisTime = r.getLong("sendTime");
			Map<String, Integer> counter = r.getMap("severness_counter", String.class, Integer.class);
			counter.forEach((key, value) -> {timeOccMapping.compute(thisTime, (k, v) -> v != null ? v + counter.get(key) : counter.get(key) );});			
		}
		
		for (Entry<Long, Integer> e : timeOccMapping.entrySet()) {
			answer += MessageFormat.format("<tr><td>{0}</td><td>{1}</td></tr>", e.getKey(), e.getValue());
		}
		answer += "</table>";
		return answer;
	}
	
	
	private static Set<String> difference(String[] current, Set<String> tracked) {
		Set<String> diffi = new HashSet<String>();
		List<String> tmp = Arrays.asList(current);
		for (String s : tracked) {
			if (!tmp.contains(s)) {
				diffi.add(s);
			}
		}
		return diffi;
	}

	/*
	 * track only for a patient a single alarm, even if it overlaps with other alarms
	 * track it through a the sets of timestamps
	 * give back a list of pairs (?) containing (alarm, timespan) that may occur more often than once!
	 *  
	 */	
	private static List<AlarmDuration> continousAlarm(int id, ResultSet info) {
		
		HashMap<Long, Set> theoreticallySorted = new HashMap<Long, Set>();
//		for (Row r : info) {
//			theoreticallySorted.put(r.getLong("sendTime"), r.getSet("reason", String.class));
//		}
		
		info.forEach(r -> theoreticallySorted.put(r.getLong("sendTime"),
												  r.getSet("reason", String.class)));
		// currently tracked stuff -> hashmap<Alarm, pair of times>
		// already done stuff -> list<(Alarm, pair of times)>
		HashMap<String, long[]> currentlyTracking = new HashMap<String, long[]>();
		List<AlarmDuration> done = new LinkedList<AlarmDuration>();
		TreeSet<Long> practicallyOrderedKeys = new TreeSet<Long>();
		practicallyOrderedKeys.addAll(theoreticallySorted.keySet());
		for (Long key : practicallyOrderedKeys) {
			int length = theoreticallySorted.get(key).size();
			Set<String> q = theoreticallySorted.get(key);
			String[] reasonsInTimestamp = q.toArray(new String[length]);
			// update of last logged timestamp

			for(String alarm: reasonsInTimestamp) {
				long[] tmp = currentlyTracking.getOrDefault(alarm, new long[] {key-4000, key});
				tmp[1] = key;
				currentlyTracking.put(alarm, tmp);
			}
			
			// identify stopped alarms
			Set<String> differences = difference(reasonsInTimestamp, currentlyTracking.keySet());
			
			// stop tracking 'invalid' alarms and save the ones that are done into the list 'done'
			for (String s : differences) {
				long[] durationStamps = currentlyTracking.get(s);
				done.add(new AlarmDuration(s, durationStamps));
				currentlyTracking.remove(s);
			}
			
		}
		
		currentlyTracking.forEach((k, v) -> { done.add(new AlarmDuration(k, v)); });
		return done;
	}
	
	/*
	 * input: (((a, t1, t2), (b, t1, t2), ..), ((a, t2, t3), (b, t2, t3), ..), ..)
	 * output {a: ([t1, t2], [t2, t3], ..), b: ([..], ..), ..}
	 */
	private static HashMap<String, List<long[]>> transformCloseness(List<List<AlarmDuration>> data) {
		HashMap<String, List<long[]>> resulting = new HashMap<String, List<long[]>>();
		for(List<AlarmDuration> sublist : data) {
			sublist.forEach(tuple -> {
				List<long[]> tmp = resulting.getOrDefault(tuple.getKey(), new LinkedList<long[]>());
				tmp.add(tuple.getValues());
				resulting.put(tuple.getKey(), tmp);
			});
		}
		return resulting;
	}
	
	/*
	 * gets the average, minimum and maximum time between alarms on either a station
	 * or a patient  - currently misleading name
	 */
	public static String closenessByTime(String patOrStat, int id, String typeOfAlarm) {
		Where stmt;
		typeOfAlarm = typeOfAlarm == null ? "" : typeOfAlarm;
		
		String answer = "<table border=2><tr><td>Type</td><td>Mean +/- StD</td><td>Max</td><td>Min</td><td>min DeltaT</td><td>max DeltaT</td></tr>";
		List<List<AlarmDuration>> completeList = new LinkedList<List<AlarmDuration>>();
		if (patOrStat.equalsIgnoreCase("patient")) {
			stmt = QueryBuilder.select().column("sendTime").column("reason")
					.from("alarm_information").where(QueryBuilder.eq("patid", id));
			ResultSet messages = session.execute(stmt);
			List<AlarmDuration> l = continousAlarm(id, messages);
			completeList.add(l);
		} else {
			List<Integer> patients = getPatientsFromStation(id, 0L);
			for (Integer patid : patients) {
				stmt = QueryBuilder.select().column("sendTime").column("reason")
						.from("alarm_information").where(QueryBuilder.eq("patid", id));
				ResultSet messages = session.execute(stmt);
				List<AlarmDuration> l = continousAlarm(patid, messages);
				completeList.add(l);
			}
		}
		// currently: (((a, t1, t2), (b, t1, t2), ..), ((a, t2, t3), (b, t2, t3), ..), ..)
		// want: 
		// all, avDur, stdDur, minDur, maxDur, minDELTA, maxDELTA 
		// a, avDur, stdDur, minDur, maxDur, minDELTA, maxDELTA
		// b, avDur, stdDur, minDur, maxDur, minDELTa, maxDELTA
		// minDELTA > 5s
		// minDuration = 5s
		
		// transform
		HashMap<String, List<long[]>> transformedData = transformCloseness(completeList);
		// apply
		HashMap<String, double[]> results = apply(transformedData, typeOfAlarm);
		// format
		for (Entry<String, double[]> e : results.entrySet()) {
			double [] r = e.getValue();
			String tmp = MessageFormat.format("<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td></tr>", r[0], r[1], r[2], r[3], r[4], r[5]);
			answer += tmp;
		}
		return answer + "</table>";
	}


	private static HashMap<String, double[]> apply(HashMap<String, List<long[]>> transformedData, String typeOfAlarm) {
		HashMap<String, double[]> toReturn = new HashMap<String, double[]>();
		
		if (!typeOfAlarm.isEmpty()) {
			 List<long[]> tldr = transformedData.get(typeOfAlarm);
			 double[] tmp = extract(tldr);
			 toReturn.put(typeOfAlarm, tmp);
		} else {
			List<Integer> completeList = new LinkedList<Integer>();
			for (Entry<String, List<long[]>> e : transformedData.entrySet()) {
				double[] tmp = extract(e.getValue());
				toReturn.put(e.getKey(), tmp);
			}
			double tmp[] = new double[6];
			tmp[0] = calculateMean(completeList);
			tmp[1] = calculateDeviation(completeList, tmp[0]);
			tmp[2] = getMinimum(toReturn.values(), 2);
			tmp[3] = getMaximum(toReturn.values(), 3);
			tmp[4] = getMinimum(toReturn.values(), 4);
			tmp[5] = getMaximum(toReturn.values(), 5);
			toReturn.put("all", tmp);		
		}
		
		return toReturn;
	}


	private static double[] extract(List<long[]> tldr) {
		double[] tmp = new double[6];
		List<Integer> mapped = new ArrayList<Integer>();
		tldr.forEach((a) -> {mapped.add( (int) (a[1] - a[0]));});
		tmp[0] = calculateMean(mapped);	
		tmp[1] = calculateDeviation(mapped, tmp[0]);
		tmp[2] = Collections.min(mapped);
		tmp[3] = Collections.max(mapped);
		tmp[4] = delta(tldr, "min");
		tmp[5] = delta(tldr, "max");

		return tmp;
	}


	private static double getMinimum(Collection<double[]> collection, int index) {
		double currentMinimum = Double.MAX_VALUE;
		for (double[] tmp : collection) {
			if (tmp[index] < currentMinimum) {
				currentMinimum = tmp[index];
			}
		}
		return currentMinimum;
	}

	private static double getMaximum(Collection<double[]> toRet, int index) {
		double currentMax = Double.MIN_VALUE;
		for (double[] tmp : toRet) {
			if (tmp[index] > currentMax) {
				currentMax = tmp[index];
			}
		}
		return currentMax;
	}

	private static double delta(List<long[]> value, String minOrMax) {
		double returnValue = 0.0;
		if (minOrMax.equals("min")) {
			returnValue = Double.MAX_VALUE;
			for (int i=0; i<value.size() - 1; i++) {
				long possibleM = value.get(i+1)[1] - value.get(i)[0];
				if (possibleM < returnValue && possibleM > 1) {
					returnValue = possibleM;
				}
			}
		} else if (minOrMax.equals("max")) {
			returnValue = Double.MIN_VALUE;
			for (int i=0; i<value.size() - 1; i++) {
				long possibleM = value.get(i+1)[1] - value.get(i)[0];
				if (possibleM > returnValue) {
					returnValue = possibleM;
				}
			}
		}
		if (returnValue == Double.MAX_VALUE || returnValue == Double.MIN_VALUE) {
			returnValue = 0.0;
		}
		return returnValue;
	}
}
