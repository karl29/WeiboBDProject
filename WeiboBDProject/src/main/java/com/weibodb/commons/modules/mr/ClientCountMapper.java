package com.weibodb.commons.modules.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.sf.json.JSONObject;

public class ClientCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	Text outputKey = new Text();
	LongWritable outputValue = new LongWritable(1L);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//parse json data
		String valuesJson = value.toString();
		if(!valuesJson.equals("")){
			JSONObject json = JSONObject.fromObject(valuesJson.substring(1, valuesJson.length() - 1));
			String source = json.getString("source");
			long timeMil = json.getLong("createTime");
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			String date = format.format(new Date(timeMil));
			outputKey.set(source + "," + date);
			context.write(outputKey, outputValue);
		}
	}
	
}
