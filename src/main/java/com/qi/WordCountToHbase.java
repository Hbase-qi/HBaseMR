package com.qi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WordCountToHbase {

	public static class WordCountToHBaseMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public static final IntWritable one=new IntWritable(1);
		public Text outputKey=new Text();
		public String[] info;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			info=value.toString().split("\\s+");
			for (String i : info) {
				if (!i.equals("")) {
					outputKey.set(i);
					context.write(outputKey, one);
				}
			}
		}
		
	}
	//定义reducer对接输出到hbase
		//reduce的输入类型KEYIN, VALUEIN
		//reduce输出的key的类型KEYOUT，写入hbase中reduce的输出key并不重要，重要的是value，value的数据会被写入hbase表，key的数据不重要
		//只需要保证reduce的输出value是put类型就可以了
		//create 'bd20:wc','i'
	public static class WordCountToHBaseReduce extends TableReducer<Text, IntWritable, NullWritable>{
		
		public static final NullWritable outPutKey=NullWritable.get();
		public Put outputValue;
		public int sum;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, NullWritable, Mutation>.Context contex)
				throws IOException, InterruptedException {
			
			sum=0;
			for (IntWritable value : values) {
				sum+=value.get();
			}
			
			outputValue=new Put(Bytes.toBytes(key.toString()));
			outputValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
			contex.write(outPutKey, outputValue);
		}
	}
	
	public static void main(String[] args) {
		
		Configuration configuration=HBaseConfiguration.create();
		Job job;
		try {
			job = Job.getInstance(configuration);
			job.setJarByClass(WordCountToHbase.class);
			job.setJobName("write data to hbase");
			
			job.setMapperClass(WordCountToHBaseMap.class);
			job.setReducerClass(WordCountToHBaseReduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			TableMapReduceUtil.initTableReducerJob("bd20:wc", WordCountToHBaseReduce.class, job);
			
			FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/employee/employee.txt"));
		
			System.exit(job.waitForCompletion(true)?0:1);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
