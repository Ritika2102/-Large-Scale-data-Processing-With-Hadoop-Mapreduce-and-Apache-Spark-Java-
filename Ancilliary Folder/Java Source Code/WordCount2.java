import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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


public class WordCount2 {
	 public static class MyMapWritable extends MapWritable {
	    @Override
	    public String toString() {
	        StringBuilder result = new StringBuilder();
	        Set<Writable> keySet = this.keySet();

	        for (Object key : keySet) {
	            result.append("{" + key.toString() + " = " + this.get(key) + "}");
	        }
	        return result.toString();
	    }
	}

  public static class TokenizerMapper extends Mapper<Object, Text, Text, MyMapWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private MyMapWritable occurrenceMap = new MyMapWritable();
  

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
       String[] line = value.toString().split("\n");
        
       String[] tokens = value.toString().split("\\s+");
       if (tokens.length > 1) {
      for (int i = 0; i < tokens.length-1; i++) {
          word.set(tokens[i]);
          occurrenceMap.clear();

           for (int j = 0; j < tokens.length; j++) {
                if (j == i) continue;
                Text neighbor = new Text(tokens[j]);
                if(occurrenceMap.containsKey(neighbor)){
                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
                  int zz=(count.get()+1);
                  count.set(zz);
                }else{
                   occurrenceMap.put(neighbor,new IntWritable(1));
                }
           }
          context.write(word,occurrenceMap);
     }
   }
  }
    }


  public static class IntSumReducer extends Reducer <Text,MyMapWritable,Text,MyMapWritable> {
    private MyMapWritable result = new MyMapWritable();
  //  private MapWritable resultingMap = new MapWritable();
    String res;
    public void reduce(Text key, Iterable<MyMapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
    	
		for (MyMapWritable val : values) 
        {
          Set<Writable> s =val.keySet();
          for(Writable k : s)
          {
            IntWritable iw =(IntWritable)val.get(k);
            if(result.containsKey(k)){
            	IntWritable count = (IntWritable)result.get(k);
                 int cc=count.get()+1;
                 count.set(cc);
            }
            else{
            	result.put(k, iw);
            }
            
          }        
        }
        
        context.write(key, result);
     
    }

  }
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}