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

import com.weibodb.commons.modules.mr.ClientCountMapper;
import com.weibodb.commons.modules.mr.ClientCountReduce;

public class ClientCountTest {
	Mapper<LongWritable,Text,Text,LongWritable> mapper;
	MapDriver<LongWritable,Text,Text,LongWritable> mapDriver;
	Reducer<Text, LongWritable, Text, LongWritable> reducer;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
	@Before
	public void init(){
		mapper = new ClientCountMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
		reducer = new ClientCountReduce();
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>(reducer);
	}
	
	@Test
	public void testMap() throws IOException{
		String value = "[{\"beCommentWeiboId\":\"\",\"beForwardWeiboId\":\"\",\"catchTime\":\"1387159495\",\"commentCount\":\"1419\","
				+ "\"content\":\"分享图片\",\"createTime\":\"1386981067000\",\"info1\":\"\",\"info2\":\"\",\"info3\":\"\",\"mlevel\":\"\",\"musicurl\":[],"
				+ "\"pic_list\":[\"http://ww3.sinaimg.cn/thumbnail/40d61044jw1ebixhnsiknj20qo0qognx.jpg\"],\"praiseCount\":\"5265\",\"reportCount\":\"1285\","
				+ "\"source\":\"iPad客户端\",\"userId\":\"1087770692\",\"videourl\":[],\"weiboId\":\"3655325888057474\",\"weiboUrl\":\"http://weibo.com/1087770692/AndhixO7g\"}]";
		Text text = new Text(value);
		LongWritable outputValue = new LongWritable(1L);
		mapDriver.withInput(outputValue, text).withOutput(new Text("iPad客户端,20131214"), new LongWritable(1L)).runTest();
		
	}
	
	@Test
	public void testReduce() throws IOException{
		Text outputKey = new Text("iPad客户端,20131214");
		ArrayList<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1L));
		values.add(new LongWritable(1L));
		values.add(new LongWritable(1L));
		reduceDriver.withInput(outputKey, values).withOutput(outputKey, new LongWritable(3)).runTest();
	}
}
