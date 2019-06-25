package com.atguigu.mapreduce.wholefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable>{
	private BytesWritable value = new BytesWritable();
	private FileSplit split;
	private Configuration configuration;
	private boolean isProcess = false;//是否正在读取文件，默认没有读取
		
	// 初始化方法
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 获取切片信息
		/*FileSplit是InputSplit的父类*/
		this.split = (FileSplit) split;
		
		// 获取配置信息
		configuration = context.getConfiguration();
	}
   /*读取源数据目录下的每一个文件*/
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		/*那就开始读取文件吧！*/
		if (!isProcess) {
			//定义一个hadoop文件输入流
			FSDataInputStream fis = null;
			try {
				//通过配置文件获取文件系统
				// 按文件整体处理，读取
				FileSystem fs = FileSystem.get(configuration);
				//通过切片信息获取切片的路径
				// 获取切片的路径
				Path path = split.getPath();
				//通过文件系统及切片的路径获取一个输入流
				// 获取到切片的输入流
				fis = fs.open(path);
				//定义一个切片大小的缓冲区
				byte[] buf = new byte[(int) split.getLength()];
				
				// 读取数据
				//nio的方式读取缓冲区中的数据
				IOUtils.readFully(fis, buf, 0, buf.length);
				
				// 设置输出
				//将缓冲区中的数据--文件内容赋值
				value.set(buf, 0, buf.length);
				
			} finally {
				IOUtils.closeStream(fis);
			}
			
			isProcess = true;
			
			return true;
		}
		
		return false;
	}
    //mr执行过程中没有key
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
