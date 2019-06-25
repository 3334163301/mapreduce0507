package com.atguigu.mapreduce.filter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*�Զ���һ�� outputformat*/
public class FilterOutputformat extends FileOutputFormat<Text, NullWritable>{
    //FilterRecordWriter�� RecordWriter<Text, NullWritable>��ʵ������
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		//ͨ�����������л��������Ķ���ʵ�������������
		// ����һ��RecordWriter
		return new FilterRecordWriter(job);
	}
}
