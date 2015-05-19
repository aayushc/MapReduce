import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Author- Aayush Chandra
 * Andrew-id- aayushc
 * course- Cloud Computing
 */
public class Test {

	private static int top=5;
	private static int remove=2;
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		//private static IntWritable one= new IntWritable(1);
		private Text word= new Text();
		private Text word2= new Text();
		private Text word3= new Text();
		private Text word4= new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
			// TODO Auto-generated method stub
			String line= value.toString();
			String[] splitted= line.split("\t");
			
			if(Integer.parseInt(splitted[1])>remove) {
								
				String[] each= splitted[0].split(" ");	
				StringBuilder sb= new StringBuilder();
				for(int i=0; i<each.length-1;i++) {
					sb.append(each[i]+" ");
				}
				word.set(sb.toString().trim());
				//one= new IntWritable(Integer.parseInt(splitted[1]));
				String val=each[each.length-1]+"\t"+splitted[1];
				word2.set(val);				
				context.write(word, word2);
				
				if(each.length<5) {
					word3.set(splitted[0]);
					word4.set(splitted[1]);
					context.write(word3, word4);
				}
			}
		}
	}
	
	//reducer class
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//int sum=0;
			for(Text value: values) {
				context.write(key, value);
			}
			//context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(Test.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
	}
}
