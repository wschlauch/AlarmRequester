package org.bitsea.AlarmRequester.utils;

public class AlarmDuration {

	String key;
	long[] values;
	
	public AlarmDuration(String s, long[] vals) {
		this.key = s;
		this.values = vals;
	}

	public String getKey() {
		return this.key;
	}
	
	public long[] getValues() {
		return this.values;
	}
	
	public long getVal1() {
		return this.values[0];
	}
	
	public long getVal2() {
		return this.values[1];
	}
}
