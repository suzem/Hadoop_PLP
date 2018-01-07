import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		String localSrc = "/home/cloudera/Downloads/arbres.csv";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				Tree arbre = new Tree(line);
				System.out.println("");
				// go to the next line
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
