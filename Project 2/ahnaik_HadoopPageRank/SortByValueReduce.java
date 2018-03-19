package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;

import indiana.cgl.hadoop.pagerank.RankRecord;
 
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByValueReduce extends Reducer<LongWritable, Text, LongWritable, DoubleWritable>{

	public void reduce(LongWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
		
		HashMap<Integer,Double> myHashMap = new HashMap<Integer,Double>();
		
		// Store nodes and rank values in HashMap in order to sort them quickly
		for (Text value: values){
			String Line = value.toString();
			RankRecord rrd = new RankRecord(Line);
			myHashMap.put(rrd.sourceUrl,rrd.rankValue);
		}
		
		List<Map.Entry<Integer,Double>> sortedHashMap = rankSortedByValues(myHashMap);
		 
		// Print the values in the sorted order
		for (Map.Entry<Integer, Double> entry : sortedHashMap)
		{
			System.out.println(entry.getKey() + "/" + entry.getValue());
			context.write(new LongWritable(entry.getKey()), new DoubleWritable(entry.getValue()));
		}
	}
	
// Implement Comparable to sort values of HashMap
static <K,V extends Comparable<? super V>> 
	List<Map.Entry<K, V>> rankSortedByValues(Map<K,V> hashmap) {
	
	List<Map.Entry<K,V>> sortedMapList = new ArrayList<Map.Entry<K,V>>(hashmap.entrySet());
	
	// Sort the HashMap using the new Comparator
	Collections.sort(sortedMapList, 
			new Comparator<Map.Entry<K,V>>() {
				@Override
				public int compare(Map.Entry<K,V> value1, Map.Entry<K,V> value2) {
					return value2.getValue().compareTo(value1.getValue());
				}
			}
		);
	return sortedMapList;
	}
}