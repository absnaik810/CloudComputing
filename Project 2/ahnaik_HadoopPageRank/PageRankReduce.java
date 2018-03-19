//PageRankReduce.java

package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.StringBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
 
public class PageRankReduce extends Reducer<LongWritable, Text, LongWritable, Text>{
	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		double sumOfRankValues = 0.0;
		String targetUrlsList = "";
		
		int sourceUrl = (int)key.get();
		int numUrls = context.getConfiguration().getInt("numUrls",1);
		
		//hint: each tuple may include rank value tuple or link relation tuple
		for (Text value: values){
			String[] strArray = value.toString().split("#");
			/*Write your code here*/

			//strArray has the list of values with the same key, like <3,#1#2> and <3,#4#5>
			// Concatenate links for the same key
			// <3,#1#2> and <3,#4#5> should form <3,#1#2#4#5>
			if (strArray.length > 1){
				targetUrlsList = targetUrlsList.concat(value.toString());
			}
			// sum up the values with the same key
			else if (value.toString().length() > 1) {
				sumOfRankValues += Double.parseDouble(value.toString());
			}
		} 
				// Calculate using the formula
		sumOfRankValues = 0.85*sumOfRankValues+0.15*(1.0)/(double)numUrls;
		
		System.out.println("Reduce Phase results: ");
		System.out.println("< " + sourceUrl + "," + sumOfRankValues + targetUrlsList + " >");
		
		context.write(key, new Text(sumOfRankValues+targetUrlsList));
	}
}