import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RoundTwoReducer extends Reducer<Text, Text, Text, Text> {

    

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {
    	
    	double max=0;
    	
    	
    	for(Text val:values){
    		String s=val.toString();
    		//double d=Double.parseDouble(s);
    		
    		System.out.println(s);
    		if(!s.equals("")  && Double.parseDouble(s)>max){
    			max=Double.parseDouble(s);
    			System.out.println("max is "+max);
    		}
    		
    		
    	}
    	
    	context.write(key, new Text(String.valueOf(max)));
    
    	
    	/*
    	Iterator<Text> iterator = values.iterator();
        //FloatWritable[] height = new FloatWritable[100];
        
        String s="";
        while (iterator.hasNext()) {
            s+=iterator.next()+";";
        }
        String[] a=s.split(";");
        float[] f=new float[a.length+1];
        for(int i=0;i<=a.length;i++){
        	f[i]=Float.parseFloat(a[i]);
        }
        Arrays.sort(f);
        //totalWordCount.set(sum);
        // context.write(key, new IntWritable(sum));
        height.set(f[a.length+1]);
        context.write(key, height);*/
    }
}