package com.atguigu.mapreduce.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable>{
	//声明两条输出路径的输出流
	private FSDataOutputStream atguiguOut = null;
	private FSDataOutputStream otherOut = null;
	/*通过任务尝试运行环境上下文对象构造过滤输出器*/
	public FilterRecordWriter(TaskAttemptContext job)  {
		/*通过任务尝试运行环境上下文对象获取Hadoop配置信息*/
		Configuration configuration = job.getConfiguration();
		
		try {
			/*通过Hadoop配置信息获取文件系统信息*/
			// 获取文件系统
			FileSystem fs = FileSystem.get(configuration);
			
			// 创建两个文件的输出流
			atguiguOut = fs.create(new Path("e:/output/atguigu.log"));
			/*指定两条过滤的输出路径位置，通过文件系统创建对应路径的输出流*/
			otherOut = fs.create(new Path("e:/output/other.log"));
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// 区分输入的key是否包含atguigu
		
		if (key.toString().contains("atguigu")) {// 包含
			/*本类实例化构造成功之后，两条路径的输出流将会被自动创建，
			* 通过对应的输出流将对应的reduce结果输出到目标路径*/
			atguiguOut.write(key.toString().getBytes());
		}else {// 不包含
			otherOut.write(key.toString().getBytes());
		}
	}
    /*只有关闭输出流之后，结果才会写进目标路径*/
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		
		if (atguiguOut != null) {
			atguiguOut.close();
		}
		
		if (otherOut != null) {
			otherOut.close();
		}
	}

}
