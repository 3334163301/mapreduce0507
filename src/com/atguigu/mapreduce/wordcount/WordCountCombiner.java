package com.atguigu.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

	// <a, 1> <a, 1>
	// <atguigu, 1> <atguigu, 1> <atguigu, 1>   ���Ϊ<atguigu,3>
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// �����ۼӺ�
		int count = 0;
		
		for(IntWritable value:values){
			count += value.get();
		}
		
		// д��
		context.write(key, new IntWritable(count));
		
	}
}