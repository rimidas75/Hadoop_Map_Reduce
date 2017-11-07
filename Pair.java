package edu.bufflo.sem2.dic.lab4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class Pair implements WritableComparable<Pair>
{

	  protected String first;
	    protected String second;

	    public Pair(String first, String second) {
	        this.first = first;
	        this.second = second;
	    }

	    public Pair() {
	       this("", "");
	    }
	    @Override
	    public void readFields(DataInput in) throws IOException {
	        first = WritableUtils.readString(in);
	        second = WritableUtils.readString(in);
	    }

	 @Override
	    public void write(DataOutput out) throws IOException {
	        WritableUtils.writeString(out, first);
	        WritableUtils.writeString(out, second);
	    }

	@Override
	public int compareTo(Pair other) {
		int cmpFirstFirst = first.compareTo(other.first);
	    int cmpSecondSecond =  second.compareTo(other.second);
	    if ( cmpFirstFirst == 0 && cmpSecondSecond == 0 ) {
	        return 0;
	    }
	    
		return 0;
	}
	 @Override
	 public String toString() {
	        return /*this.hashCode() + "\t" +*/ first + "\t" + second;
	    }

	  
}
