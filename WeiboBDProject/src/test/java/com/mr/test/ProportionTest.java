package com.mr.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.weibodb.commons.modules.mr.EmotionProportionMapper;

public class ProportionTest {
	Mapper<LongWritable, Text, Text, Text> mapper;
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	
	@Before
	public void init(){
		mapper = new EmotionProportionMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>(mapper);
	}
	
	@Test
	public void testMap() throws IOException{
		String value = "1182419921,20130202	4,105,174,238";
		Text outputValue = new Text(value);
		LongWritable outputKey = new LongWritable(1L);
		mapDriver.withInput(outputKey, outputValue).withOutput(new Text("1182419921,20130202"), new Text("0.01680672268907563,0.4411764705882353,0.7310924369747899"))
		.runTest();
	}
}
