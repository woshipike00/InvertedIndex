import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndex {
		
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private IntWritable one=new IntWritable(1);
		private Text content=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String filename=((FileSplit)context.getInputSplit()).getPath().getName();
			String line=value.toString();
			StringTokenizer tokenizer=new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()){
				String word=tokenizer.nextToken();
				content.set(word+":"+filename);
				context.write(content, one);
			}
		}
		
		
	}
	
	public static class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text value, Iterable<IntWritable> itr,
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			for(IntWritable num:itr){
				sum+=num.get();
			}
			context.write(value, new IntWritable(sum));
		}
		
	}
	
	public static class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			// TODO Auto-generated method stub
			String newkey=key.toString().split(":")[0];
			return super.getPartition(new Text(newkey), value, numReduceTasks);
		}
		
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text , Text>{
		
		private static String pretext=null;
		private static List<String> plist=new ArrayList<String>();

		@Override
		protected void reduce(Text value, Iterable<IntWritable> itr,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			String curtext=value.toString().split(":")[0];
			String docid=value.toString().split(":")[1];
			for(IntWritable num:itr){
				sum+=num.get();
			}
			
			if(pretext==null){
				pretext=curtext;
				plist.add(docid+":"+sum);
			}
			else {
				if(pretext.equals(curtext)){
					plist.add(docid+":"+sum);
				}
				else {
					StringBuffer buffer=new StringBuffer();
					for(String ele:plist){
						buffer.append(ele+"\t");
					}
					context.write(new Text(pretext),new Text(buffer.toString()));
					plist.clear();
					pretext=curtext;
					plist.add(docid+":"+sum);
				}
			}
			
			
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			StringBuffer buffer=new StringBuffer();
			for(String ele:plist){
				buffer.append(ele+"\t");
			}
			context.write(new Text(pretext),new Text(buffer.toString()));
			plist.clear();
		}
		
		
		
		
		
	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
		
		Configuration conf=new Configuration();
		String[] otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherargs.length!=2){
			System.out.println("args error!");
		}
		
		Job job=new Job(conf,"InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setPartitionerClass(InvertedIndexPartitioner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
