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


public class Part2Test {

	private static int top=5;
	private static int remove=2;
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		//private static IntWritable one= new IntWritable(1);
		private Text word= new Text();
		private Text word2= new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
			// TODO Auto-generated method stub
			String line= value.toString();
			String[] splitted= line.split("\t");
			
			String[] each= splitted[0].split(" ");			
			
			word.set(each[0]);
			//one= new IntWritable(Integer.parseInt(splitted[1]));
			word2.set(line);
			
			context.write(word, word2);
		}
	}
	
	public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
		
		HashMap<String, Integer> map= new HashMap<String, Integer>();
		HashMap<String, Double> map1= new HashMap<String, Double>();
		@Override
		protected void reduce(Text key,Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			//int count=0;
			//HashMap<String, Integer> map= new HashMap<String, Integer>();
			
			for(Text t:values) {
				String[] text= t.toString().split("\t");
				int count= Integer.parseInt(text[1]);
				if(count>2) {
					map.put(text[0], count);									
				}
			}
			
			for(String s:map.keySet()) {
				StringBuilder sb= new StringBuilder();
				String[] each= s.split(" ");
				if(each.length>1) {
					for(int i=0; i<each.length-1;i++) {
						sb.append(each[i]+" ");
					}
					if(map.get(sb.toString().trim())!=null) {
						int total= map.get(sb.toString().trim());
						double probability = (double)map.get(s)/ total;
						map1.put(s, probability);
					}
				}
				
			}
			
			ArrayList<String> two= new ArrayList<String>();
			ArrayList<String> three= new ArrayList<String>();
			ArrayList<String> four= new ArrayList<String>();
			ArrayList<String> five= new ArrayList<String>();
			for(String s: map1.keySet()) {
				String[] st= s.split(" ");
				if(st.length==2) two.add(s);
				if(st.length==3) three.add(s);
				if(st.length==4) four.add(s);
				if(st.length==5) five.add(s);
			}
			if(two.size()!=0) sort(two,context);
			if(three.size()!=0) sort(three,context);
			if(four.size()!=0) sort(four,context);
			if(five.size()!=0) sort(five,context);
			map.clear();
			map1.clear();
			
		}
		
		private void sort(ArrayList<String> al,Context context) throws IOException, InterruptedException {
			Collections.sort(al, new Comparator<String>() {

				@Override
				public int compare(String s1, String s2) {
					// TODO Auto-generated method stub
					if((map.get(s1) - map.get(s2))!=0) {
						return (map.get(s1) - map.get(s2));
					} else {
						return s1.compareTo(s2);
					}
				}
			});
			//	Collections.reverse(al);
			String[] s= al.get(0).split(" ");
			StringBuilder sbr= new StringBuilder();
			for(int k=0; k<s.length-1;k++) {
				sbr.append(s[k]+" ");
			}
			Put put= new Put(Bytes.toBytes(sbr.toString().trim())); 
			for(int i=0 ;i<Math.min(top, al.size());i++) {				
				String[] check=al.get(i).split(" ");
				String value=""+map1.get(al.get(i));
				put.add(Bytes.toBytes("cf"), Bytes.toBytes(check[check.length-1]), Bytes.toBytes(value));
				//put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes()));				
				
			}
			context.write(null, put);
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
