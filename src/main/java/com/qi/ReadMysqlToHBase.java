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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

public class ReadMysqlToHBase {

	public static class MyMapper extends Mapper<LongWritable, User, User, NullWritable> {

		@Override
		protected void map(LongWritable key, User value, Mapper<LongWritable, User, User, NullWritable>.Context context)
				throws IOException, InterruptedException {

			context.write(value, NullWritable.get());
		}
	}

	public static class myReducer extends TableReducer<User, NullWritable, NullWritable> {

		public static final NullWritable outPutKey = NullWritable.get();
		public Put outputValue;

		@Override
		protected void reduce(User key, Iterable<NullWritable> values,
				Reducer<User, NullWritable, NullWritable, Mutation>.Context contex)
				throws IOException, InterruptedException {

			outputValue = new Put(Bytes.toBytes(key.getId()));
			outputValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("name"), Bytes.toBytes(key.getName()));
			outputValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(key.getAge()));
			outputValue.addColumn(Bytes.toBytes("i"), Bytes.toBytes("id"), Bytes.toBytes(key.getId()));
			contex.write(outPutKey, outputValue);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration configuration = HBaseConfiguration.create();
		DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/hadoop_db",
				"root", "123456");
		
		Job job=Job.getInstance(configuration);
		job.setJarByClass(ReadMysqlToHBase.class);
		job.setJobName("ReadMysqlToHBase");
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(User.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		DBInputFormat.setInput(job, User.class, "user", null, "id", "id","name","age");
		TableMapReduceUtil.initTableReducerJob("bd20:wc",myReducer.class, job);
		job.addFileToClassPath(new Path("hdfs://master:9000/mysql-connector-java-5.1.38.jar"));
		
		System.exit(job.waitForCompletion(true)? 0:1);
		
	}
}
