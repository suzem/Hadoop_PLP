package driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Pagerank_Driver {

    private static String source= "";
    private static String link = "";
    private static String rank = "";

    public static void main(String[] args) {
        Pagerank_Driver client = new Pagerank_Driver();

        if (args.length == 3) {
            rank = args[2];

            link = args[1];
            source = args[0];
        }

        try {
            client.execute();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    private void execute() throws Exception {

        String tempjoinPath="tempJoinPath";
        runprepareJob(source, link,rank+"0");

        for (int i=0;i<30;i++){
            //join the data of "linkpages" and the last result of "rank"
            runIntegrateJob(link, rank+String.valueOf(i), tempjoinPath);
            runcontriJob(tempjoinPath,rank+String.valueOf(i+1));
        }
        //sort the result of "rank" in descending order
        runSortJob(rank+"30",rank+"30_sort");
    }



}
