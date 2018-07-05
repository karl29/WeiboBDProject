package com.weibodb.commons.modules.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClientCountReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
	LongWritable outputValue = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable val : value){
			sum += val.get();
		}
		
		outputValue.set(sum);
		context.write(key, outputValue);
	}
	
}
