package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class FreqIndexBuilderClueWeb09 {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class FibMapper extends TableMapper<ImmutableBytesWritable, Writable> {		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] docIdBytes = rowKey.get();
			byte[] contentBytes = result.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			
			// TODO: write your implementation for getting the term frequencies from each document, and generating Put objects for 
			// clueWeb09IndexTable. 
            // Hint: use the "getTermFreqs" function to count the frequencies of terms in content.
            // The schema of the clueWeb09IndexTable is:
            // row key: term, column family: "frequencies", qualifier: document Id, cell value: term frequency in the corresponding document
            // Check iu.pti.hbaseapp.Constants for useful constant values.

			//Creating a HashMap to store <word, frequency> pair
			HashMap< String, Integer > hm = getTermFreqs(content);
			
			for(Map.Entry< String, Integer > ent : hm.entrySet()) {
				//Creating a Put object for each <word, frequency> pair
				Put p = new Put(Bytes.toBytes(ent.getKey()));
				
				//Calling add() function to add the column family, qualifier and the values
				p.add(Constants.CF_FREQUENCIES_BYTES, docIdBytes, Bytes.toBytes(ent.getValue()));

				//Calling write() function of context to finally insert the data into the HBase table
				context.write(new ImmutableBytesWritable(p.getRow()), p);
			}
		}
	}
	
	/**
	 * get the terms, their frequencies and positions in a given string using a Lucene analyzer
	 * @param text
	 * 
	 */
	public static HashMap<String, Integer> getTermFreqs(String text) {
		HashMap<String, Integer> freqs = new HashMap<String, Integer>();
		try {
			Analyzer analyzer = Constants.analyzer;
			TokenStream ts = analyzer.reusableTokenStream("dummyField", new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (Helpers.isNumberString(termVal)) {
					continue;
				}
				
				if (freqs.containsKey(termVal)) {
					freqs.put(termVal, freqs.get(termVal)+1);
				} else {
					freqs.put(termVal, 1);
				}
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return freqs; 
	}
	
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
	    Scan scan = new Scan();
	    scan.addColumn(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
		Job job = new Job(conf,	"Building freq_index from " + Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(FibMapper.class);
		TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_DATA_TABLE_NAME, scan, FibMapper.class, ImmutableBytesWritable.class, Writable.class, job, true);
		TableMapReduceUtil.initTableReducerJob(Constants.CLUEWEB09_INDEX_TABLE_NAME, null, job);
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
