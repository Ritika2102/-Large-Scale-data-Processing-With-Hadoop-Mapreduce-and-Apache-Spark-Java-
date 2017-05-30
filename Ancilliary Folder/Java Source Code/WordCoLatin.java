
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCoLatin {
	static HashMap<String, String> LemmaMap = new HashMap<String, String>(); 

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String[] newstring = value.toString().split(">");
    	 if(newstring.length>1){
    	String firstpart = newstring[0]+">";
    	String remaining = newstring[1].trim();
    	String arr[]=remaining.split(" ");
    	 for(int j=0;j<arr.length-1;j++){
    		 String newarr = arr[j];
    		 String str="";
	    		String twostr="";
	    		str = newarr.replace("j", "i");
	    		twostr=str.replace("v", "u");
	    	for(int i=j+1;i<arr.length;i++){
	    		if(i==j){
	    			continue;
	    		}
	    		String neighbor = arr[i];
	    		String str1="";
	    		String twostr1="";
	    		str1 = neighbor.replace("j", "i");
	    		twostr1=str1.replace("v", "u");
	    		
	    		String finalgoing=twostr+","+twostr1;
	    		
	    	   context.write(new Text(finalgoing),new Text(firstpart));
	    		
     }
    	 }
    	 
    }
    }
  }


  public static class IntSumReducer extends Reducer <Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String[] keyscome = key.toString().split(",");
      if(keyscome.length>1){
      String part1=keyscome[0];
      String part2= keyscome[1];
    	String going = "";
    	String keyToWrite = "";
    
        	if(LemmaMap.containsKey(part1)){
        		if(LemmaMap.containsKey(part2)){
        		keyToWrite="<"+LemmaMap.get(part1)+","+LemmaMap.get(part2)+">";
        		for (Text val : values) 
                { 
                      going = going+val.toString(); 
                     
                }
        		 context.write(new Text(keyToWrite), new Text(going));
        	}
        		else{
        			keyToWrite="<"+LemmaMap.get(part1)+","+part2+">";
            		for (Text val : values) 
                    { 
                          going = going+val.toString(); 
                         
                    }
            		 context.write(new Text(keyToWrite), new Text(going));	 
        		}
        	}
        	else if(LemmaMap.containsKey(part2)){
        		if(LemmaMap.containsKey(part1)){
            		keyToWrite="<"+LemmaMap.get(part1)+","+LemmaMap.get(part2)+">";
            		for (Text val : values) 
                    { 
                          going = going+val.toString(); 
                          
                    }
            		context.write(new Text(keyToWrite), new Text(going));
            	}
            		else{
            			keyToWrite="<"+part1+","+LemmaMap.get(part2)+">";
                		for (Text val : values) 
                        { 
                              going = going+val.toString(); 
                        }
                		 context.write(new Text(keyToWrite), new Text(going));	
            		}
        	}
        	else{
        		keyToWrite="<"+part1+","+part2+">";
        		for (Text val : values) 
                {
        			going = going+val.toString(); 
                      
                }
        		context.write(new Text(keyToWrite), new Text(going));
        	}
      }
    }

  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   
    Job job = Job.getInstance(conf, "word count");
   // FileSystem fs = FileSystem.get(conf);
   // FSDataInputStream inputStream = fs.open(new Path("C:/Users/Fnu/Downloads/la.lexicon.csv"));
    BufferedReader	br = new BufferedReader(new FileReader("/home/hadoop/la.lexicon.csv"));
    while(br.readLine()!=null){
    	String fileRead= br.readLine();
    	if(fileRead!=null){
    		String part []= fileRead.split(",");
        	String keyToAdd = part[0];
        	String valueToAdd = part[2];
        	LemmaMap.put(keyToAdd, valueToAdd);
    	}
    	
    }
    job.setJarByClass(WordCoLatin.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
