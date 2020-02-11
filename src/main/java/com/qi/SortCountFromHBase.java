package com.qi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortCountFromHBase {

	public static class SortCountFromHbaseMap extends TableMapper<IntWritable, Text>{
		
		private IntWritable outputKey=new IntWritable();
		private Text outputValue=new Text();
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
		
			String word=Bytes.toString(value.getRow());
			String count = Bytes.toString(value.getValue(Bytes.toBytes("i"), Bytes.toBytes("count")));
			outputKey.set(Integer.valueOf(count));
			outputValue.set(word);
			context.write(outputKey, outputValue);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration configuration=HBaseConfiguration.create();
		Job job=Job.getInstance(configuration);
		job.setJarByClass(SortCountFromHBase.class);
		job.setJobName("read from hbase to hdfs");
		
		Scan scan=new Scan();
		scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("count"));
		TableMapReduceUtil.initTableMapperJob("bd20:wc", scan, SortCountFromHbaseMap.class, IntWritable.class, Text.class, job);
		
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		Path outputDir=new Path("hdfs://master:9000/wcfromhbase");
		
		outputDir.getFileSystem(configuration).delete(outputDir,true);
		FileOutputFormat.setOutputPath(job, outputDir);
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
	
}
