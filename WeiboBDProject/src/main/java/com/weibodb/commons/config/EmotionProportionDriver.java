package com.weibodb.commons.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.weibodb.commons.constant.Constant;
import com.weibodb.commons.modules.mr.EmotionProportionMapper;

public class EmotionProportionDriver {
	public static void main(String[] args) {
		if(args.length < 2){
			System.out.println("please set input and output path");
			System.exit(0);
		}
		Configuration config = new Configuration();
		try {
			Job job = Job.getInstance(config);
			
			job.setJarByClass(EmotionProportionDriver.class);
			job.setMapperClass(EmotionProportionMapper.class);
			//bu xuyao reudce
			job.setNumReduceTasks(Constant.REDUCE_NUM);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//map output compress
			config.setBoolean("mapreduce.map.output.compress", true);
			config.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);
			//reduce compress
			config.setBoolean("mapreduce.output.fileoutputformat.compress", true);
			config.setClass("mapreduce.output.fileoutputformat.compress.codec", GzipCodec.class, CompressionCodec.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
