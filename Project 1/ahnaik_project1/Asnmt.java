import java.lang.*;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Asnmt {

  public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); // type of output value
    private Text word = new Text();   // type of output key

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());    // set word as each input keyword
        context.write(word, one);     // create a pair <keyword, 1>
      }
    }
  }

/*	So now, the output of map() is something like below:
 *	<0.81, 2>
 *	<0.80, 6>
 *      <0.11, 1>
 *	etc.  
 *
 *
 */

 public static class Reduce
	extends Reducer<Text , IntWritable, Text, DoubleWritable>{
	private FloatWritable result = new FloatWritable();
        double count=0.0d, average=0.0d, curKey=0.0d, max=-Double.MAX_VALUE, min=Double.MAX_VALUE, sum=0.0d, stdv=0.0d, varSum=0.0d;
	int curVal = 0;

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		for(IntWritable val : values){
			curKey = Double.parseDouble(key.toString());
			curVal = val.get();
			count += val.get();
			sum += (curKey * curVal);
			varSum += Math.pow(curKey,2) * curVal;
		    //comparing the curKey to check if it is less than minVal
			//If yes, then update the value of minVal
			if(curKey < min)
				min = curKey;
			//comparing the curKey to check if it is more than maxVal
			//If yes, then update the value of maxVal
			if(curKey > max)
				max = curKey;
		}
	
	}

	
	public void display(Context context) throws IOException, InterruptedException{
	//calculate average as sum/count (number of values)
	average = sum/count;
	//below calculation is for the standard deviation
	double a = Math.pow(average,2);
	double b = varSum/count;
	double c = b-a;
	stdv = Math.sqrt(c);

	context.write(new Text("Minimum is: "), new DoubleWritable(min));
	context.write(new Text("Maximum is: "), new DoubleWritable(max));
	context.write(new Text("Average is: "), new DoubleWritable(average));
	context.write(new Text("Standard Deviation is: "), new DoubleWritable(stdv));
	}
}

  public static class Combiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	//This is just a copy-paste of the original Reduce logic from WordCount

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; // initialize the sum for each keyword
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);

      context.write(key, result); // create a pair <keyword, number of occurences>
    }
  }


  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }

    // create a job with name "wordcount"
    Job job = new Job(conf, "average");
    job.setJarByClass(Asnmt.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Combiner.class);
    job.setReducerClass(Reduce.class);

    // Add a combiner here, not required to successfully run the wordcount program

    // set output key type
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(IntWritable.class);
    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
