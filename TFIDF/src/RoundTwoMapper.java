import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RoundTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString().replaceAll("    ", " "); 
        int index = val.indexOf(" ");
        String s1 = val.substring(0, index); 
        String s2 = val.substring(index + 1);
        s2 += " ";
        s2 += "1";
        context.write(new Text(s1), new Text(s2));
    }
}
