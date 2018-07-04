package com.weibodb.commons.modules.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmotionProportionMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text outputKey = new Text();
	Text outputValue = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] result = value.toString().split("\t");
		String[] lines = result[1].split(",");
		double negPropor = 0d;
		double posPropor = 0d;
		double naturePropor = 0d;
		Double allCommentsNum = Double.valueOf(lines[3].trim());
		//posNum + "," + negNum + "," + natureNum + "," + allCommentsNum
		if(allCommentsNum != 0d){
			negPropor = Double.valueOf(lines[1])/allCommentsNum;
			posPropor = Double.valueOf(lines[0])/allCommentsNum;
			naturePropor = Double.valueOf(lines[2])/allCommentsNum;
		}
		outputKey.set(result[0]);
		outputValue.set(posPropor + "," + negPropor + "," + naturePropor);
		
		context.write(outputKey, outputValue);
	}
	
}
