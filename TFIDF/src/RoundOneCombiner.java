import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RoundOneCombiner extends Reducer<Text, Text, Text, Text> {
    float all = 0;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int index = key.toString().indexOf(" ");
        if (key.toString().substring(index + 1, index + 2).equals("!")) {
            for (Text val : values) {
                all = Integer.parseInt(val.toString());
            }
            return;
        }
        float sum = 0;
        for (Text val : values) {
            sum += Integer.parseInt(val.toString());
        }
        float tmp = sum / all;
        String value = "";
        value += tmp; 
        String p[] = key.toString().split(" ");
        String key_to = "";
        key_to += p[1];
        key_to += " ";
        key_to += p[0];
        context.write(new Text(key_to), new Text(value));
    }
}