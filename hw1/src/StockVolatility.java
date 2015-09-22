import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockVolatility {
	public static void main(String[] args) throws Exception{
		double start=(double) System.currentTimeMillis();
		try
		{
			//Double start = (double) System.currentTimeMillis();
			
			Job job = Job.getInstance();
		    job.setJarByClass(StockVolatility.class);
			
			job.setMapperClass(StockVolatilityMapper.class);
			job.setCombinerClass(StockVolatilityReducer.class);
			job.setReducerClass(StockVolatilityReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path("small"));
			FileOutputFormat.setOutputPath(job, new Path("output"));
			
			job.setJarByClass(StockVolatility.class);
			job.waitForCompletion(true);
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
