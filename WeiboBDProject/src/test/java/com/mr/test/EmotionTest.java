package com.mr.test;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.weibodb.commons.modules.mr.EmotionMapper;
import com.weibodb.commons.modules.mr.EmotionReducer;

public class EmotionTest {
	Mapper<LongWritable, Text, Text, Text> mapper;
	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	Reducer<Text, Text, Text, Text> reducer;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	
	@Before
	public void init(){
		mapper = new EmotionMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>(mapper);
		reducer = new EmotionReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>(reducer);
	}
	
	@Test
	public void testMap() throws IOException {
		String value = "1387166484,1378354542,283,1,2195,1182419921,3.61914E+15,4,105,174";
		Text inputValue = new Text(value);
		
		LongWritable inputKey = new LongWritable(1L);
		mapDriver.withInput(inputKey,inputValue)
		.withOutput(new Text("1182419921,20130905"), new Text("105,4,174,283")).runTest();
	}
	@Test
	public void testReduce() throws IOException{
		String key = "1182419921,20130905";
		Text inputValue = new Text(key);
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("1,1,1,3"));
		values.add(new Text("1,1,1,3"));
		values.add(new Text("1,1,1,3"));
		reduceDriver.withInput(inputValue, values).withOutput(new Text(key), new Text("3,3,3,9")).runTest();
	}
}
