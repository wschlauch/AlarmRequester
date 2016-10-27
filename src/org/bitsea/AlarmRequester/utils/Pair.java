package org.bitsea.AlarmRequester.utils;


import java.util.Objects;

public class Pair extends Object {

	public float left;
	public float right;
	
	public Pair(float left, float right) {
		this.left = left;
		this.right=right;
	}
	
	
	@Override
	public String toString() {
		return "(" + left + "," + right + ")";
	}

	  
	@Override
	public boolean equals(Object other) {
		if (other == null) {
			return false;
		}
		if (other == this) {
			return true;
		}
		if (!(other instanceof Pair)) {
			return false;
		}
		Pair other_ = (Pair) other;
		return Objects.equals(other_.left, this.left) && Objects.equals(other_.right, this.right);
	}

}
