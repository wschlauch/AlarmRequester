package org.bitsea.AlarmRequester;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.bitsea.AlarmRequester.utils.AlarmDuration;
import org.bitsea.AlarmRequester.utils.TransformationUtil;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;

import io.netty.handler.codec.ByteToMessageDecoder.Cumulator;

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
	
	
	private static String getStation(int statID) {
		Where stmt = QueryBuilder.select().column("stationName").from("station_name")
				.where(QueryBuilder.eq("stationID", statID));
		ResultSet rs = session.execute(stmt);
		String idx = "";
		if (!rs.isExhausted()) {
			idx = rs.one().getString("stationName");
		} 
		return idx;
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
	
	
	private static TupleValue calculateTuple(List<TupleValue> ls) {
		int changes = 0;
		double m = 0.0;
		double s = 0.0;
		for (TupleValue val : ls) {
			m += val.getDouble(0);
			changes += val.getBool(1) ? 1: 0;
		}
		m = m/ls.size();
		for (TupleValue val : ls) {
			s += Math.pow(val.getDouble(0) - m, 2); 
		}
		s = Math.sqrt(s/ls.size());
		TupleType transportType = session.getCluster().getMetadata().newTupleType(DataType.cdouble(), 
				DataType.cdouble(), DataType.cint());
		TupleValue toReturn = transportType.newValue();
		toReturn.setDouble(0, m);
		toReturn.setDouble(1, s);
		toReturn.setInt(2, changes);
		return toReturn;
		
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
		String statName = getStation(StationID);
		stmt = QueryBuilder.select().column("patid").column("time")
				.from("patient_bed_station").allowFiltering()
				.where(QueryBuilder.eq("station", statName));
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
			finalResult += MessageFormat.format("<tr><td>{0}</td><td>{1} +/- {2}</td><td>{3}</td></tr>", severness,
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
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
		String answer = "<table border=2><tr><td>Time</td><td>Severness</td><td>Count</td></tr>";
		
		// for each timestamp, get simultaneous alarms (i.e. sum over array per person, average over all of these)
		// do this with a mapping
		TreeMap<Long, Integer> timeOccMapping = new TreeMap<Long, Integer>();
		TreeMap<Long, String> timeSevMapping = new TreeMap<Long, String>();
		for (Row r : results) {
			long thisTime = r.getLong("sendTime");
			Map<String, Integer> counter = r.getMap("severness_counter", String.class, Integer.class);
			counter.forEach((key, value) -> {timeOccMapping.compute(thisTime, (k, v) -> timeOccMapping.containsKey(k) ?  v + counter.get(key) : counter.get(key) );});
			counter.forEach((key, value) -> {timeSevMapping.compute(thisTime, (k, v) -> v == null ? key : v + ", " + key);});
		}
		
		for (Entry<Long, Integer> e : timeOccMapping.entrySet()) {
			answer += MessageFormat.format("<tr><td>{0}</td><td>{2}</td><td>{1}</td></tr>", format.format(new Date(e.getKey())), e.getValue(), timeSevMapping.get(e.getKey()));
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
		
		HashMap<Long, Set<String>> theoreticallySorted = new HashMap<Long, Set<String>>();
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
	 * gets the average, minimum and maximum time between alarms on either a station
	 * or a patient  - currently misleading name
	 */
	public static String closenessByTime(String patOrStat, int id, String typeOfAlarm) {
		Where stmt;
		typeOfAlarm = typeOfAlarm == null ? "" : typeOfAlarm;
		
		String answer = "<table border=2><tr><td>Type</td><td>Mean +/- StD</td><td>Min</td><td>Max</td><td>min DeltaT</td><td>max DeltaT</td></tr>";
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
						.from("alarm_information").where(QueryBuilder.eq("patid", patid));
				ResultSet messages = session.execute(stmt);
				List<AlarmDuration> l = continousAlarm((int) patid, messages);
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
		HashMap<String, List<long[]>> transformedData = TransformationUtil.transformCloseness(completeList);
		
		// apply
		TreeMap<String, double[]> results = apply(transformedData, typeOfAlarm);
		
		// format
		if (typeOfAlarm.isEmpty()) {
			double[] r = results.get("all");
			String tmp = MessageFormat.format("<tr><td>{6}</td><td>{0} +/- {1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td></tr>", r[0], r[1], r[2], r[3], r[4], r[5], "All Alarms");
			answer += tmp;
		}
		
		for (Entry<String, double[]> e : results.entrySet()) {
			if (e.getKey().equalsIgnoreCase("all")) { continue; }
			double[] r = e.getValue();
			String tmp = MessageFormat.format("<tr><td>{6}</td><td>{0} +/- {1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td></tr>", r[0], r[1], r[2], r[3], r[4], r[5], e.getKey());
			answer += tmp;
		}
		return answer + "</table>";
	}


	private static TreeMap<String, double[]> apply(HashMap<String, List<long[]>> transformedData, String typeOfAlarm) {
		TreeMap<String, double[]> toReturn = new TreeMap<String, double[]>();
		
		if (!typeOfAlarm.isEmpty()) {
			try {
			 List<long[]> tldr = transformedData.get(typeOfAlarm);
			 double[] tmp = extract(tldr);
			 toReturn.put(typeOfAlarm, tmp);
			} catch (NullPointerException c) {
				// do nothing, should show up empty handed
			}
		} else {
			List<Integer> completeList = new LinkedList<Integer>();
			for (Entry<String, List<long[]>> e : transformedData.entrySet()) {
				double[] tmp = extract(e.getValue());
				toReturn.put(e.getKey(), tmp);
				e.getValue().forEach((a) -> {completeList.add((int) (a[1] - a[0]));});
			}
			double tmp[] = new double[6];
			tmp[0] = calculateMean(completeList);
			tmp[1] = calculateDeviation(completeList, tmp[0]);
			tmp[2] = TransformationUtil.getMinimum(toReturn.values(), 2);
			tmp[3] = TransformationUtil.getMaximum(toReturn.values(), 3);
			tmp[4] = TransformationUtil.getMinimum(toReturn.values(), 4);
			tmp[5] = TransformationUtil.getMaximum(toReturn.values(), 5);
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
		tmp[4] = TransformationUtil.delta(tldr, "min");
		tmp[5] = TransformationUtil.delta(tldr, "max");

		return tmp;
	}


	public static String implausibleAlarms(String patOrStat, int id) {
		Where stmt;
		Where stmt2;
		if (patOrStat.equalsIgnoreCase("patient")) {
			stmt = QueryBuilder.select().column("numeric").column("patid").from("oru_messages")
					.where(QueryBuilder.eq("patid", id));
			stmt2 = QueryBuilder.select().all().from("patient_standard_values")
					.where(QueryBuilder.eq("patid", id));
		} else {
			List<Integer> patients = getPatientsFromStation(id, null);
			stmt = QueryBuilder.select().column("numeric").column("patid").from("oru_messages")
					.where(QueryBuilder.in("patid", patients));
			stmt2 = QueryBuilder.select().all().from("patient_standard_values")
					.where(QueryBuilder.in("patid", patients));
		}
		
		// got all the information, i.e. measurements as well as borders
		// for each patient from station check the standard values against
		// the measurement. If more than 50% too high/low -> implausible alarm
		// thus, generically build a mapping {patid : {border1:(a,b), border2:(a,b),  ..}, ..}
		ResultSet rs = session.execute(stmt2);
		HashMap<Integer, Map<String, TupleValue>> patBorders = composeStandardValues(rs);
		HashMap<String, Integer> overshot = new HashMap<String, Integer>();
		HashMap<String, Integer> within = new HashMap<String, Integer>();
		HashMap<String, Integer> alarm = new HashMap<String, Integer>();
		Set<String> reasons = new HashSet<String>();
		rs = session.execute(stmt);
		for (Row row : rs) {
			int patid = row.getInt("patid");
			Map<String, TupleValue> mapping = patBorders.get(patid);
			Map<String, TupleValue> data = row.getMap("numeric", String.class, TupleValue.class);
			for (Entry<String, TupleValue> tpl : data.entrySet()) {
				TupleValue borders = mapping.get(tpl.getKey());
				float toCompare = tpl.getValue().getFloat(0);
				if (borders == null) {continue;}
				if (toCompare <= 0.5*borders.getFloat(0) || toCompare >= 1.5*borders.getFloat(1)) {
					overshot.compute(tpl.getKey(), (k, v) -> v == null ? 1 : v+1);
					reasons.add(tpl.getKey());
				} else if (toCompare <= borders.getFloat(1) && toCompare >= borders.getFloat(0)) {
					within.compute(tpl.getKey(), (k, v) -> v == null ? 1 : v+1);
					reasons.add(tpl.getKey());
				} else {
					alarm.compute(tpl.getKey(), (k, v) -> v==null? 1 : v+1);
					reasons.add(tpl.getKey());
				}
			}
		}
		
		// now to display
		String answer = "<table border=2><tr><td>Alarm</td><td>Within Borders</td><td>Alarm</td><td>Implausible</td></tr>";
		for (String reason : reasons) {
			answer += MessageFormat.format("<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td></tr>", 
					reason, within.getOrDefault(reason, 0), alarm.getOrDefault(reason, 0), overshot.getOrDefault(reason, 0));
		}
		return answer + "</table>";
	}


	private static HashMap<Integer, Map<String, TupleValue>> composeStandardValues(ResultSet rs) {
		HashMap<Integer, Map<String,TupleValue>> pBorders = new HashMap<Integer, Map<String,TupleValue>>();
		for (Row row : rs) {
			Integer patID = row.getInt("patid");
			Map<String, TupleValue> params = row.getMap("parameters", String.class, TupleValue.class);
			pBorders.put(patID, params);
		}
		return pBorders;
	}

	/*
	 * get latest alarm (or alarm at timestamp) and try to discover if the changes occured
	 * via therapeutic measures (i.e. change of borders, medicamentation, care)
	 * for this, check whether there are newer patient_standard_values or whether the alarm
	 * was stopped in a reasonable time frame
	 * variables: timepoint at which the alarm occurred, standard last alarm
	 * alarmtype, standard all
	 * 
	 * reasonable time frame not defined, thus take average time as 50%; if alarm stopped within 1std, 
	 * it can be assumed to be done by personal
	 */
	public static String reactionToAlarm(String patOrStat, int id, Object o1, Object o2) {
		// differentiate the optional Objects into their correct type
		long time = -1L;
		String alarmType = "";

		if (o1 != null) {
			if (o1 instanceof Long) {
				time = (long) o1;
			} else if (o1 instanceof String) {
				alarmType = (String) o1;
			}
		}
		if (o2 != null) {
			if (o2 instanceof Long) {
				time = (long) o2;
			} else if (o2 instanceof String) {
				alarmType = (String) o2;
			}
		}
		
		List<TupleValue> rslts = new ArrayList<TupleValue>();
		
		if (patOrStat.equalsIgnoreCase("patient")) {
			rslts.add(patientChangedTherapy(id, time, alarmType));
		} else {
			List<Integer> patients = getPatientsFromStation(id, time);
			for (Integer pid: patients) {
				rslts.add(patientChangedTherapy(pid, time, alarmType));
			}
		}
		
		// currently, list of tuple value containing a) probability that someone reacted to alarm
		// and whether the therapy was changed after alarm begin
		// return the average probability of a reaction, standard deviation, how often did a change occur
		String answer = "<table border=2><tr><td>Average probability</td><td>Std. deviation</td><td># changes</td></tr>";
		TupleValue results = calculateTuple(rslts);
		answer += "<tr><td>" + Math.round(results.getDouble(0)*100) + "%</td><td>" + Math.round(results.getDouble(1)*100) + "%</td><td>" + 
				results.getInt(2) + " of " + rslts.size() + "</td></tr></table>";
		return answer;		
	}
	
	
	/*
	 * does actually belong in an extra class or in the patient class?
	 * 
	 */
	private static TupleValue patientChangedTherapy(int patid, long time, String alarmType) {
		// get latest alarm or end of alarm around time
//		Where stmt = QueryBuilder.select().column("").from("alarm_information")
//				.where(QueryBuilder.eq("patid", patid));
//		if (time != -1L) {
//			stmt.and(QueryBuilder.gt("sendTime", time)).limit(1);
//		} else {
//			stmt.and(QueryBuilder.lt("receivedTime", System.currentTimeMillis())).limit(1);
//		}
		
		
		
		// get all alarms for patient to identify mean time
		Where stmt = QueryBuilder.select().column("sendTime").column("reason")
				.from("alarm_information").where(QueryBuilder.eq("patid", patid));
		ResultSet messages = session.execute(stmt);
		// contains tuples (alarm, startTime, endTime)
		List<AlarmDuration> alarmsOfPatient = continousAlarm(patid, messages);
		HashMap<String, List<long[]>> resulting = new HashMap<String, List<long[]>>();
		
		// calc times in alarm state
		alarmsOfPatient.forEach(tuple -> {
			List<long[]> tmp = resulting.getOrDefault(tuple.getKey(), new LinkedList<long[]>());
			tmp.add(tuple.getValues());
			resulting.put(tuple.getKey(), tmp);
		});
		List<Integer> x = new ArrayList<Integer>();
		resulting.forEach((k, v) -> {for(long[] q: v) {
			x.add((int) (q[1] - q[0]));
		};});
		
		double meanTme = calculateMean(x);
		double stdTime = calculateDeviation(x, meanTme);
		
		// select the alarms around the time given; if not time is given (-1L)
		// select the longest last alarm
		List<Long> durationInterstingStamps = new ArrayList<Long>();
		List<Long> startTimes = new ArrayList<Long>();
		if (time != -1) {
			for ( List<long[]> tmp : resulting.values()) {
				durationInterstingStamps.addAll(tmp.stream().filter(v -> v[0] < time && time < v[1]).map(new Function<long[], Long>() {
					public Long apply(long[] v) {
						return v[1] - v[0];
					}
				}).collect(Collectors.toList()));
				startTimes.addAll(tmp.stream().filter(v -> v[0] < time && time < v[1]).map(new Function<long[], Long>() {
					public Long apply(long[] v) {
						return v[0];
					}
				}).collect(Collectors.toList()));
			}
		} else {
			// if no time given, select from each group the alarm with the latest stamp
			// not very elegant, check whether mapping and filtering makes this more elegant
			for (List<long[]> tmp : resulting.values()) {
				long[] max = new long[] {0L, 0L};
				for (long[] e : tmp) {
					if (e[1] > max[1]) { max = e;}
				}
				
				durationInterstingStamps.add(max[1] - max[0]);
				startTimes.add(max[0]);
				//tmp.stream().map(new Function<long[], Long>() {public Long apply(long[] v) {return v[1];}}).max(Comparator.comparingLong(i -> i));
			}
		}
		
		
		// compare to average time
		// under the assumption that alarm times are normally distributed
		long interestingTime = Collections.max(durationInterstingStamps);
		double xn = stdTime > 0 ? Math.abs(interestingTime - meanTme) / stdTime : 0.0;
		
		NormalDistribution d;
		double result = 0.0;
		if (stdTime > 0.0) { 
			d = new NormalDistribution(meanTme, stdTime);
			result = 1 - d.cumulativeProbability(xn);
		}
		// resulting probability, but this is the wrong approach. 
		
		// check whether standard border values have been updated in the meantime
		stmt = QueryBuilder.select().column("sendTime").from("patient_standard_values")
				.where(QueryBuilder.eq("patid", patid)).and(QueryBuilder.gt("sendTime", time));
		ResultSet rs = session.execute(stmt);
		boolean changed = false;
		long minStartTimes = Collections.min(startTimes); 
		for (Row r: rs) {
			long wasSendAt = r.getLong("sendTime");
			if (wasSendAt > minStartTimes) {
				changed = true;
			}
			
		}
		// if any of the two above apply, it is true that there was some action (pure assumption!)
		TupleType transportType = session.getCluster().getMetadata().newTupleType(DataType.cdouble(), DataType.cboolean());
		TupleValue toReturn = transportType.newValue();
		toReturn.setDouble(0, result);
		toReturn.setBool(1, changed);
		return toReturn;
		
	}
	

}
