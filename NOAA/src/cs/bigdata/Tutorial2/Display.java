package cs.bigdata.Tutorial2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.util.*;



public class Display {

	public static void main(String[] args) throws IOException {
		
		
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = fs.open(new Path("isd-history.txt"));
		long compteurLine=0;
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			
			
			while (line !=null){
				compteurLine++;
				
				if(compteurLine>22){
					char[] array=line.toCharArray();
					String usaf="";
					String nameOfStation="";
					String ctry="";
					String elevation="";
			
					for(int i=0;i<=5;i++){
						char a=line.charAt(i);
						usaf+=a;
						//usaf+=String.valueOf(a);
						//System.out.println(a);
						
					}
					for(int i=12;i<=41;i++){
						char b=line.charAt(i);
						nameOfStation+=b;
					}
					for(int i=42;i<=44;i++){
						char c=line.charAt(i);
						ctry+=c;
					}
					for(int i=73;i<=80;i++){
						char d=line.charAt(i);
						elevation+=d;
					}
					System.out.println("USAF: "+usaf+" ,the name of the station: "+nameOfStation+" ,the country: "+ctry+" ,the elevation: "+elevation+".");
					
					//System.out.println(line);
					//System.out.println(usaf);
					
				}

				line = br.readLine();
			}
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
 
		
		
	}

}
