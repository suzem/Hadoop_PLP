import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RoundOnePartitioner extends Partitioner<Text, Text> {
    public int getPartition(Text key, Text value, int numPartitions) {
        String ip1 = key.toString();
        ip1 = ip1.substring(0, ip1.indexOf(" "));
        Text p1 = new Text(ip1);
        return Math.abs((p1.hashCode() * 127) % numPartitions);
    }
}