package com.weibodb.commons.modules.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  example:
   input: "1387166484	1378354542	283	1	2195	1182419921	3.61914E+15	4	105	174"
   output: 1182419921+date, 4	105	174
 * */
public class EmotionMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text outputKey = new Text();
	Text outputValue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] lines = value.toString().split(",");
		
		String userId = lines[5].trim();
		
		String negNum = lines[7].trim();
		
		String posNum = lines[8].trim();
		
		String natureNum = lines[9].trim();
		
		String allCommentsNum = lines[2].trim();
		
		long catchTime = Long.valueOf(lines[1].trim()) * 1000;
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String date = format.format(new Date(catchTime));
		
		String outputKey1 = userId + "," + date;
		String outputValue1 = posNum + "," + negNum + "," + natureNum + "," + allCommentsNum;
		outputKey.set(outputKey1);
		outputValue.set(outputValue1);
		context.write(outputKey, outputValue);
	}
	
}
