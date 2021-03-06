import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class StockVolatilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	static double start=(double) System.currentTimeMillis();
	static Text word = new Text(); // output key
	static double[] maxVolatility=new double[11];
	static String[] maxVolatilityKey=new String[11];
	static double[] minVolatility=new double[11];
	static String[] minVolatilityKey=new String[11];
	//double sum = 0.0;
	static int check=0;
	static Context ctx;
	static String previousKey="";
	static double sum = 0.0;
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		
		
		for (DoubleWritable val: values){
			sum += 1.0;
			double value=val.get();
			if(!(check>10)){
				maxVolatility[check]=value;
				maxVolatilityKey[check]=key.toString();
				minVolatility[check]=value;
				minVolatilityKey[check]=key.toString();
				check++;
				if(check==11){
					for(int i=0;i<11;i++){
						for(int j=0;j<10;j++){
							if(maxVolatility[j]<maxVolatility[j+1]){
								double temp = maxVolatility[j+1];
								String tmp = maxVolatilityKey[j+1];
								maxVolatility[j+1]=maxVolatility[j];
								maxVolatilityKey[j+1]=maxVolatilityKey[j];
								maxVolatility[j]=temp;
								maxVolatilityKey[j]=tmp;
							}
						}
					}
					for(int i=0;i<11;i++){
						for(int j=0;j<10;j++){
							if(minVolatility[j]>minVolatility[j+1]){
								double temp = minVolatility[j+1];
								String tmp = minVolatilityKey[j+1];
								minVolatility[j+1]=minVolatility[j];
								minVolatilityKey[j+1]=minVolatilityKey[j];
								minVolatility[j]=temp;
								minVolatilityKey[j]=tmp;
							}
						}
					}
					check++;
				}
			}
			else{
				String s=key.toString();
				if(!s.equals(maxVolatilityKey[0]) && !s.equals(maxVolatilityKey[1]) && !s.equals(maxVolatilityKey[2]) && !s.equals(maxVolatilityKey[3]) && !s.equals(maxVolatilityKey[4]) && !s.equals(maxVolatilityKey[5]) && !s.equals(maxVolatilityKey[6]) && !s.equals(maxVolatilityKey[7]) && !s.equals(maxVolatilityKey[8]) && !s.equals(maxVolatilityKey[9]) && !s.equals(maxVolatilityKey[10]) && !s.equals(minVolatilityKey[0]) && !s.equals(minVolatilityKey[1]) && !s.equals(minVolatilityKey[2]) && !s.equals(minVolatilityKey[3]) && !s.equals(minVolatilityKey[4]) && !s.equals(minVolatilityKey[5]) && !s.equals(minVolatilityKey[6]) && !s.equals(minVolatilityKey[7]) && !s.equals(minVolatilityKey[8]) && !s.equals(minVolatilityKey[9]) && !s.equals(minVolatilityKey[10])){
					int i=9,j=10;
					if(maxVolatility[10]<value){
						for(i=9;i>=0;i--){
							if(maxVolatility[i]>value){
								for(j=10;j>i;j--){
									maxVolatility[j]=maxVolatility[j-1];
									maxVolatilityKey[j]=maxVolatilityKey[j-1];
								}
								maxVolatility[j+1]=value;
								maxVolatilityKey[j+1]=key.toString();
								break;
							}
						}
						if(i==-1){
							for(j=10;j>0;j--){
								maxVolatility[j]=maxVolatility[j-1];
								maxVolatilityKey[j]=maxVolatilityKey[j-1];
							}
							maxVolatility[j]=value;
							maxVolatilityKey[j]=key.toString();
						}
					}
					
					if(minVolatility[10]>value){
						for(i=9;i>=0;i--){
							if(minVolatility[i]<value){
								for(j=10;j>i;j--){
									minVolatility[j]=minVolatility[j-1];
									minVolatilityKey[j]=minVolatilityKey[j-1];
								}
								minVolatility[j+1]=value;
								minVolatilityKey[j+1]=key.toString();
								break;
							}
						}
						if(i==-1){
							for(j=10;j>0;j--){
								minVolatility[j]=minVolatility[j-1];
								minVolatilityKey[j]=minVolatilityKey[j-1];
							}
							minVolatility[j]=value;
							minVolatilityKey[j]=key.toString();
						}
					}
				}
			}
		}
		//context.write(key, new DoubleWritable(sum));
		//context.write(key, new DoubleWritable(sum));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//context.write(word, new DoubleWritable(1.1));
		DoubleWritable dw=new DoubleWritable();
		maxVolatility[10]=100.0;
		maxVolatilityKey[10]="Above mentioed results are for stocks with maximum volatility";
		minVolatility[10]=0.0;
		minVolatilityKey[10]="Above mentioed results are for stocks with minimum volatility";
		for(int i=0;i<11;i++){
			//context.write(word, new DoubleWritable(1.1));
			dw.set(maxVolatility[i]);
			if(maxVolatilityKey[i]!=null){
				word.set(maxVolatilityKey[i]);
				context.write(word,dw);
			}
		}
		
		for(int i=0;i<11;i++){
			//context.write(word, new DoubleWritable(1.1));
			dw.set(minVolatility[i]);
			if(maxVolatilityKey[i]!=null){
				word.set(minVolatilityKey[i]);
				context.write(word,dw);
			}
		}
		word.set("Time taken in seconds to run the program");
		dw.set((System.currentTimeMillis()-start)/1000);
		context.write(word,dw);
	}
	
}
