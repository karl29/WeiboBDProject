package com.weibodb.commons.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.weibodb.commons.modules.mr.ClientCountMapper;
import com.weibodb.commons.modules.mr.ClientCountReduce;

public class ClientCountDriver extends Configured implements Tool{

	public int run(String[] arg0) throws Exception {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(ClientCountDriver.class);
		job.setMapperClass(ClientCountMapper.class);
		job.setReducerClass(ClientCountReduce.class);
		//map compress
		config.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		config.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
		
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new ClientCountDriver(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
