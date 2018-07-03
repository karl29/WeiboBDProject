package com.weibodb.commons.modules.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * example:
   input: "1182419921+date, <4	105	174>,<>"
   output: 1182419921+date,
 * 
 * */
public class EmotionReducer extends Reducer<Text, Text, Text, Text>{
	Text outputValue = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		long negNum = 0;
		long posNum = 0;
		long natureNum = 0;
		long allCommentsNum = 0;
		for(Text text : value){
			String[] textValues = text.toString().split(",");
			negNum += Long.valueOf(textValues[1]);
			posNum += Long.valueOf(textValues[0]);
			natureNum += Long.valueOf(textValues[2]);
			allCommentsNum += Long.valueOf(textValues[3]);
		}
		
		outputValue.set(negNum + "," + posNum + "," + natureNum + "," + allCommentsNum);
		context.write(key, outputValue); 
	}
	
}
