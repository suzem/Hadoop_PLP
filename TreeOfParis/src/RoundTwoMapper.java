import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//To complete according to your problem
public class RoundTwoMapper extends Mapper<Object, Text, Text, Text> {
	
	
//Overriding of the map method
@Override
	protected void map(Object key, Text val, Context context) throws IOException,InterruptedException
	 {
	     // To complete according to the processing
	     // Processing : keyI = ..., valI = ...
			String line = val.toString();
			//CSVReader R=new CSVReader(new StringReader(line));
			if(line.startsWith("(")){
				String[] array=line.split(";");
				String s=array[2];
				String h=array[6];
				
				context.write(new Text(s),new Text(h));
			}
			
			
			
		/*	
			StringTokenizer tokenizer = new StringTokenizer(s);
			//StringTokenizer tokenizer1 = new StringTokenizer(hauteur);
			while(tokenizer.hasMoreTokens())
			{
				type.set(tokenizer.nextToken());
				//height.set(tokenizer1.nextToken());
				context.write(type, height);
			}*/
	 }

	public void run(Context context) throws IOException, InterruptedException {
	 setup(context);
	 while (context.nextKeyValue()) {
	     map(context.getCurrentKey(), context.getCurrentValue(), context);
	 }
	 cleanup(context);
	}

}

