//WordCountClueWeb09.java

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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordCountClueWeb09 {
	
	static class WcMapper extends TableMapper<Text, LongWritable> {
		@Override
		public void map(ImmutableBytesWritable row, Result result,	Context context) throws IOException, InterruptedException {
			byte[] contentBytes = result.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			
			// TODO: write your implementation for counting words in each row, and generating a <word, count> pair
			// Hint: use the "getWordFreq" function to count the frequencies of words in content

			// Get Hash of all frequencies
			HashMap<String, Long> hm = getWordFreq(content);			
			// Use the entrySet() function as shown in this StackOverflow answer: http://stackoverflow.com/a/1066607/2172854
			for (Map.Entry<String, Long> ent : hm.entrySet()){
				// Display the output
				System.out.println("<Key, " + ent.getKey() + "Value> : " + ent.getValue());
				
				// Write it to the context object
				context.write(new Text(ent.getKey()), new LongWritable(ent.getValue()));
			}
		}
	}

    public static class WcReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
    	@Override
        public void reduce(Text word, Iterable<LongWritable> freqs, Context context)
                throws IOException, InterruptedException {
            // TODO: write your implementation for getting the final count of each word and putting it into the 
        	// word count table
            // Hint -- the schema of the WordCountTable is: 
            //   rowkey: a word, column family: "frequencies", column name: "count", cell value: count of the word
            // Check iu.pti.hbaseapp.Constants for the constant values to use.

    		long totalFreq = 0;
    		
    		// Calculate the total frequency for the word
    		for (LongWritable ent : freqs){
    			totalFreq += ent.get();
    		}
    		
    		// Create a new Put instance
    		Put pt = new Put(Bytes.toBytes(word.toString()));
    		
    		// Add column family, name and value respectively
			//The constants have been retrieved from the file iu.pti.hbaseapp.Constants
    		pt.add(Constants.CF_FREQUENCIES_BYTES, Constants.QUAL_COUNT_BYTES, Bytes.toBytes(totalFreq));
			
    		// Write it to the context object
    		context.write(null, pt);
        }
    }
    
    /**
	 * Tokenize the given "text" with a Lucene analyzer, count the frequencies of all the words in "text", and
	 * return a map from the words to their frequencies.
	 * @param text A string to be tokenized.
	 * @return 
	 */
	public static HashMap<String, Long> getWordFreq(String text) {
		HashMap<String, Long> freqs = new HashMap<String, Long>();
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
					freqs.put(termVal, 1L);
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
	    Scan scan = new Scan();
	    scan.addColumn(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
		Job job = new Job(conf,	"Counting words from " + Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(WordCountClueWeb09.class);	
		TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_DATA_TABLE_NAME, scan, WcMapper.class, Text.class, LongWritable.class, job, true);
		TableMapReduceUtil.initTableReducerJob(Constants.WORD_COUNT_TABLE_NAME, WcReducer.class, job);	
		job.setNumReduceTasks(4);
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}