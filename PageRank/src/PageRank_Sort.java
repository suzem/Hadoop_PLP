
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRank_Sort {

    public static class SortMapper extends Mapper<Object, Text,  DoubleWritable, Text> {
        private DoubleWritable rank_double=new DoubleWritable();
        private Text newvalue = new Text();

        protected void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {
            String str[]=value.toString().split("\t");
            String rank=str[1].substring(str[1].indexOf("*")+1,str[1].lastIndexOf("*"));
            rank_double.set(Double.parseDouble(rank));
            newvalue.set(str[0]);
            context.write( rank_double,newvalue);
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, Text,DoubleWritable> {

        protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            for(Text val : values){
                context.write(val, key);
            }
        }
    }

    public static class DescComparator extends Comparator{
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                           int arg4, int arg5) {
            return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
        }
        public int compare(WritableComparable a,WritableComparable b){
            return -super.compare(a,b);
        }
    }

