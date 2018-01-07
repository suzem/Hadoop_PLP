import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//To complete according to your problem
public class RoundOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text type = new Text();
//Overriding of the map method
@Override
	protected void map(LongWritable key, Text val, Context context) throws IOException,InterruptedException
	 {
	     // To complete according to the processing
	     // Processing : keyI = ..., valI = ...
			String line = val.toString();
			//CSVReader R=new CSVReader(new StringReader(line));
			String[] array=line.split(";");
			String s=array[2];
			StringTokenizer tokenizer = new StringTokenizer(s);
			while(tokenizer.hasMoreTokens())
			{
				type.set(tokenizer.nextToken());
				context.write(type, one);
			}
	 }

	public void run(Context context) throws IOException, InterruptedException {
	 setup(context);
	 while (context.nextKeyValue()) {
	     map(context.getCurrentKey(), context.getCurrentValue(), context);
	 }
	 cleanup(context);
	}

}

