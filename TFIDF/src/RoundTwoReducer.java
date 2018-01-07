import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RoundTwoReducer extends Reducer<Text, Text, Text, Text> {
    int file_count;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        file_count = context.getNumReduceTasks(); 
        float sum = 0;
        List<String> vals = new ArrayList<String>();
        for (Text str : values) {
            int index = str.toString().lastIndexOf(" ");
            sum += Integer.parseInt(str.toString().substring(index + 1)); 
            vals.add(str.toString().substring(0, index));
        }
        double tmp = Math.log10(file_count * 1.0 / (sum * 1.0)); 
        
        for (int j = 0; j < vals.size(); j++) {
            String val = vals.get(j);
            String end = val.substring(val.lastIndexOf("\t")+1);
            float f_end = Float.parseFloat(end);
            val += " ";
            val += f_end * tmp; 
            context.write(key, new Text(val));
        }
    }
}