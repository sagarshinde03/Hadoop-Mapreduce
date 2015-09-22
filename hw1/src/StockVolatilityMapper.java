import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class StockVolatilityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	private static IntWritable one = new IntWritable(1); // value = 1
	private static Text word = new Text(); // output key
	private static DoubleWritable dw=new DoubleWritable();
	private static int currentMonth=0;
	private static String fileName="";
	private static double lastAdjClose=0.0;
	private static double firstAdjClose=0.0;
	private static double[] monthlyRateOfReturn=new double[100];//check whether all elements are 0.0
	private static int monthSequence=0;
	private static int numberOfMonths=0;
	
	public void map(LongWritable key, Text value, Context context){
		try
		{
			int lastCommaIndex;
			int lastIndexOfSlash=context.getInputSplit().toString().lastIndexOf("/");
			int lastIndexOfDot=context.getInputSplit().toString().lastIndexOf(".");
			String newfileName = context.getInputSplit().toString().substring(lastIndexOfSlash+1, lastIndexOfDot);
			if(fileName.equals(""))
				fileName = newfileName;
			else if(!fileName.equals(newfileName)){
				//code to calculate volatility of previous company and write into hashmap and then set all attributes like day,month,array to null/0
				for(int i=0;i<100;i++){
					if(monthlyRateOfReturn[i]!=0.0)
						numberOfMonths++;
				}
				monthlyRateOfReturn[numberOfMonths]=(lastAdjClose-firstAdjClose)/firstAdjClose;
				numberOfMonths++;
				double xBar=0.0;
				for(int i=0;i<numberOfMonths;i++){
					xBar+=monthlyRateOfReturn[i];
				}
				xBar=xBar/numberOfMonths;
				double difference,differenceSquare,partialVolatility,volatility;
				double sumOfDifferenceSquare=0.0;
				for(int i=0;i<numberOfMonths;i++){
					difference=monthlyRateOfReturn[i]-xBar;
					differenceSquare=difference*difference;
					sumOfDifferenceSquare+=differenceSquare;
				}
				partialVolatility=sumOfDifferenceSquare/(numberOfMonths-1);
				volatility=Math.sqrt(partialVolatility);
				
				//setting all variables to their default values
				numberOfMonths=0;
				monthlyRateOfReturn=new double[100];
				currentMonth=0;
				lastAdjClose=0.0;
				firstAdjClose=0.0;
				monthSequence=0;
				//logic ends
				word.set(fileName);
				dw.set(volatility);
				context.write(word, dw);
				fileName = newfileName;
			}
			//starting logic that if month is changed(irrespective of whether year id changed or not) then calculate monthly rate of return
			String line = value.toString();
			if(!line.startsWith("Date"))
			{
				//StringTokenizer tokenizer = new StringTokenizer(line,","); // based on Comma
				String[] result = line.split(",");
				String[] dateComponents=result[0].split("/");
				if(dateComponents[0]==result[0])
					dateComponents=result[0].split("-");
				int x=0;
				if(dateComponents[0].length()==4)
					x++;
				int newMonth=Integer.parseInt(dateComponents[x]);
				int newDay = Integer.parseInt(dateComponents[x+1]);
				if(currentMonth==0){
					currentMonth=newMonth;
					lastAdjClose=Double.parseDouble(result[6]);
					firstAdjClose=lastAdjClose;
				}
				else if(currentMonth!=newMonth){
					//logic to calculate monthly return and store lastAdjClose of current month
					monthlyRateOfReturn[monthSequence]=(lastAdjClose-firstAdjClose)/firstAdjClose;
					monthSequence++;
					//logic ends
					currentMonth=newMonth;
					lastAdjClose=Double.parseDouble(result[6]);
					firstAdjClose=lastAdjClose;
				}
				else{
					firstAdjClose=Double.parseDouble(result[6]);
				}
			}
			
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	/*
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//context.write(word, new DoubleWritable(1.1));
		DoubleWritable dw=new DoubleWritable();
		int x=0;
		x=1;
	}
	*/
}
