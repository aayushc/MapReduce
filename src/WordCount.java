import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Author- Aayush Chandra
 * Andrew-id- aayushc
 * course- Cloud Computing
 */
public class WordCount {

	//Mapper class
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one= new IntWritable(1);
		private Text word= new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line= value.toString();
			String[] splitAll= line.split("[^a-zA-Z]+");
			String[] splitted= new String[splitAll.length];
			for(int i=0; i<splitAll.length;i++) {
				//convert to lower case
				splitted[i]= splitAll[i].toLowerCase();
			}
			for(int i=0; i<splitted.length;i++) {
				//write to context each words and 1
				if(!splitted[i].trim().isEmpty()) {
					word.set(splitted[i]);
					context.write(word, one);
					if(i>=1) {
						word.set(splitted[i-1]+" "+splitted[i]);
						context.write(word, one);							
					}
					if(i>=2) {
						word.set(splitted[i-2]+" "+splitted[i-1]+" "+splitted[i]);
						context.write(word, one);
					}
					if(i>=3) {
						word.set(splitted[i-3]+" "+splitted[i-2]+" "+splitted[i-1]+" "+splitted[i]);
						context.write(word, one);
					}
					if(i>=4) {
						word.set(splitted[i-4]+" "+splitted[i-3]+" "+splitted[i-2]+" "+splitted[i-1]+" "+splitted[i]);
						context.write(word, one);
					}
				}
			}
		}
	}
	
	//reducer class
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			for(IntWritable value: values) {
				sum +=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
	}
}
