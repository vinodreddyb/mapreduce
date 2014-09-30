package com.vinod.hadoop.mapreduce.example.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BiGram implements WritableComparable<BiGram> {

	private Text first;
	private Text second;

	@Override
	public void readFields(DataInput input) throws IOException {
		this.first = new Text(input.readUTF());
		this.second = new Text(input.readUTF());

	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(first.toString());
		output.writeUTF(second.toString());
	}
	
	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public int compareTo(BiGram o) {
		int result = this.first.compareTo(o.first);
		if (result == 0) {
			result = this.second.compareTo(o.second);
		}
		return result;
	}

	
	

	@Override
	public String toString() {
		return  first + " " + second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BiGram other = (BiGram) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	public Text getFirst() {
        return first;
    }
 
    public Text getSecond() {
        return second;
    }
}
