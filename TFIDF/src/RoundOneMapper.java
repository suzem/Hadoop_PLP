import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RoundOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    String File_name = ""; 
    int all = 0;
    static Text one = new Text("1");
    String word;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        String str = ((FileSplit) inputSplit).getPath().toString();
        File_name = str.substring(str.lastIndexOf("/") + 1);
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word = File_name;
            word += " ";
            word += itr.nextToken();
            all++;
            context.write(new Text(word), one);
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        String str = "";
        str += all;
        context.write(new Text(File_name + " " + "!"), new Text(str));
    }
}