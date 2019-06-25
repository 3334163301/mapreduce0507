package com.atguigu.mapreduce.filter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*自定义一个 outputformat*/
public class FilterOutputformat extends FileOutputFormat<Text, NullWritable>{
    //FilterRecordWriter是 RecordWriter<Text, NullWritable>的实现子类
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		//通过任务尝试运行环境上下文对象实例化过滤输出器
		// 创建一个RecordWriter
		return new FilterRecordWriter(job);
	}
}
