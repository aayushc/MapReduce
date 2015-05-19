import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Project4_1Part2 {

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
	
	public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
		
		HashMap<String, Integer> map= new HashMap<String, Integer>();
		HashMap<String, Double> map1= new HashMap<String, Double>();
		@Override
		protected void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			
			int total=0;
			String keyVal="";
			for(Text t:values) {
				if(t!=null && !t.toString().isEmpty()) {
					String[] splitted= t.toString().split("\t");				
					if(splitted.length==1) {
						total= Integer.parseInt(splitted[0]);
					} else {
						map.put(splitted[0], Integer.parseInt(splitted[1]));
						/*double probability=(double)Integer.parseInt(splitted[1])/total;
						map1.put(splitted[0], probability);*/
					}	
				}
			}
			
			for(String s:map.keySet()) {
				if(total!=0) {
					double probability= (double)map.get(s)/total;
					map1.put(s, probability);
				}
			}
			
			ArrayList<String> al= new ArrayList<String>();
			for(String s:map1.keySet()) {
				al.add(s);
			}
			
			Collections.sort(al,new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					// TODO Auto-generated method stub
					double diff=map.get(s1)-map.get(s2);
					if(diff>0.0) return 1;
					else if(diff<0.0) return -1;
					else return s1.compareTo(s2);
				}
			});
			if(al.size()!=0) {
				Put put= new Put(Bytes.toBytes(key.toString())); 
				for(int i=0; i<Math.min(top, al.size());i++) {
					String val=""+map1.get(al.get(i));
					put.add(Bytes.toBytes("cf"), Bytes.toBytes(al.get(i)), Bytes.toBytes(val));
				}
				context.write(null, put);
			}
			map.clear();
			map1.clear();
		}	
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf= HBaseConfiguration.create();
		

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job= new Job(conf, "WordCountPart2");
		job.setJarByClass(Project4_1Part2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ImmutableBytesWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		top= Integer.parseInt(otherArgs[1]);
		remove= Integer.parseInt(otherArgs[2]);
		String tableName= otherArgs[3];
		
		TableMapReduceUtil.initTableReducerJob(tableName, Reduce.class, job);
		
		job.waitForCompletion(true);
		
	}
}
