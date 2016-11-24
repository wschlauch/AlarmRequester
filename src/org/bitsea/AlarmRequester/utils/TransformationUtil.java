package org.bitsea.AlarmRequester.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class TransformationUtil {

	/*
	 * input: (((a, t1, t2), (b, t1, t2), ..), ((a, t2, t3), (b, t2, t3), ..), ..)
	 * output {a: ([t1, t2], [t2, t3], ..), b: ([..], ..), ..}
	 */
	public static HashMap<String, List<long[]>> transformCloseness(List<List<AlarmDuration>> data) {
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
	

	public static double getMinimum(Collection<double[]> collection, int index) {
		double currentMinimum = Double.MAX_VALUE;
		for (double[] tmp : collection) {
			if (tmp[index] < currentMinimum) {
				currentMinimum = tmp[index];
			}
		}
		return currentMinimum;
	}

	public static double getMaximum(Collection<double[]> toRet, int index) {
		double currentMax = Double.MIN_VALUE;
		for (double[] tmp : toRet) {
			if (tmp[index] > currentMax) {
				currentMax = tmp[index];
			}
		}
		return currentMax;
	}

	/*
	 * used to calculate the delta between timepoints
	 * can calculate max delta and min delta
	 */
	public static double delta(List<long[]> value, String minOrMax) {
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
