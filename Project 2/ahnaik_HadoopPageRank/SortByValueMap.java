package indiana.cgl.hadoop.pagerank.helper;

import indiana.cgl.hadoop.pagerank.RankRecord;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortByValueMap extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
			//Submit same key with values
			context.write(new LongWritable(1), value);
		}
}