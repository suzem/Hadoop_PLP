import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TreeOfParis {

    public static void main(String[] args) throws Exception {
        
    	//Round One
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus p[] = hdfs.listStatus(new Path(args[0]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(TreeOfParis.class);
        job.setMapperClass(RoundOneMapper.class);
        job.setReducerClass(RoundOneReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextInputFormat.class);
        job.setNumReduceTasks(p.length);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (hdfs.exists(new Path(args[1]))) {
            hdfs.delete(new Path(args[1]), true);
        }
        job.waitForCompletion(true);
        
        
        
        
        
        /////////
    	
    	Configuration conf2 = new Configuration();
        
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(TreeOfParis.class);
        job2.setMapperClass(RoundTwoMapper.class);
        job2.setReducerClass(RoundTwoReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(p.length);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (hdfs.exists(new Path(args[2]))) {
            hdfs.delete(new Path(args[2]), true);
        }
        job2.waitForCompletion(true);
        
    	
    	
       
        
        
    }
}
