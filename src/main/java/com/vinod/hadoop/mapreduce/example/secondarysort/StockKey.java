package com.vinod.hadoop.mapreduce.example.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StockKey implements WritableComparable<StockKey>{

	private Text stockSymbol;
	private Long timeStamp;
	
	public StockKey() {
		
	}
	public StockKey(Text stockSymbol, Long timeStamp) {
		super();
		this.stockSymbol = stockSymbol;
		this.timeStamp = timeStamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stockSymbol = new Text(in.readUTF());
		timeStamp = in.readLong();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(stockSymbol.toString());
		out.writeLong(timeStamp);
	}

	@Override
	public int compareTo(StockKey o) {
		int result = stockSymbol.compareTo(o.stockSymbol);
		if(0 == result) {
			result = timeStamp.compareTo(o.timeStamp);
		}
		return result;
		
	}

	@Override
	public String toString() {
		return "StockKey [stockSymbol=" + stockSymbol + ", timeStamp="
				+ timeStamp + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stockSymbol == null) ? 0 : stockSymbol.hashCode());
		result = prime * result
				+ ((timeStamp == null) ? 0 : timeStamp.hashCode());
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
		StockKey other = (StockKey) obj;
		if (stockSymbol == null) {
			if (other.stockSymbol != null)
				return false;
		} else if (!stockSymbol.equals(other.stockSymbol))
			return false;
		if (timeStamp == null) {
			if (other.timeStamp != null)
				return false;
		} else if (!timeStamp.equals(other.timeStamp))
			return false;
		return true;
	}
	public Text getStockSymbol() {
		return stockSymbol;
	}
	public void setStockSymbol(Text stockSymbol) {
		this.stockSymbol = stockSymbol;
	}
	public Long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(Long timeStamp) {
		this.timeStamp = timeStamp;
	}
	

}
