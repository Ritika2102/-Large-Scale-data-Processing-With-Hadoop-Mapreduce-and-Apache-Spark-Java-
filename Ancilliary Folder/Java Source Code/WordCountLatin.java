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


public class WordCountLatin {
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
    	
	    	for(int i=0;i<arr.length;i++){
	    		String newarr = arr[i];
	    		String str="";
	    		String twostr="";
	    		str = newarr.replace("j", "i");
	    		twostr=str.replace("v", "u");
	    		if(twostr!=null || !twostr.contains(" ")){
	    	   context.write(new Text(twostr),new Text(firstpart));
	    		}
     }
    	 
    }
    }
  }


  public static class IntSumReducer extends Reducer <Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
    	String going = "";
    	
        	if(LemmaMap.containsKey(key)){
        		for (Text val : values) 
                {
                      going = going+val.toString(); 
                     
                }
        		 context.write(new Text(LemmaMap.get(key)), new Text(going));
        	}
        	else{
        		for (Text val : values) 
                {
                      going = going+val.toString(); 
                      
                }
        		context.write(new Text(key), new Text(going));
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
    job.setJarByClass(WordCountLatin.class);
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