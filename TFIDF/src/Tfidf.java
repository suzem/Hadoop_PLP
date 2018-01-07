import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tfidf {

    public static void main(String[] args) throws Exception {
        
    	//Round One
        Configuration conf1 = new Configuration();
        FileSystem hdfs = FileSystem.get(conf1);
        FileStatus p[] = hdfs.listStatus(new Path(args[0]));
        Job job1 = Job.getInstance(conf1, "TFIDF_Round_One");
        job1.setJarByClass(Tfidf.class);
        job1.setMapperClass(RoundOneMapper.class);
        job1.setCombinerClass(RoundOneCombiner.class);
        job1.setReducerClass(RoundOneReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(p.length);
        job1.setPartitionerClass(RoundOnePartitioner.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (hdfs.exists(new Path(args[1]))) {
            hdfs.delete(new Path(args[1]), true);
        }
        job1.waitForCompletion(true);
        
        
        //Round Two
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "TFIDF_Round_Two");
        job2.setJarByClass(Tfidf.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(RoundTwoMapper.class);
        job2.setReducerClass(RoundTwoReducer.class);
        job2.setNumReduceTasks(p.length);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (hdfs.exists(new Path(args[2]))) {
            hdfs.delete(new Path(args[2]), true);
        }
        job2.waitForCompletion(true);
    }
}
