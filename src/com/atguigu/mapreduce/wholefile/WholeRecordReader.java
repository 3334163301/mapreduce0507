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
	private boolean isProcess = false;//�Ƿ����ڶ�ȡ�ļ���Ĭ��û�ж�ȡ
		
	// ��ʼ������
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// ��ȡ��Ƭ��Ϣ
		/*FileSplit��InputSplit�ĸ���*/
		this.split = (FileSplit) split;
		
		// ��ȡ������Ϣ
		configuration = context.getConfiguration();
	}
   /*��ȡԴ����Ŀ¼�µ�ÿһ���ļ�*/
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		/*�ǾͿ�ʼ��ȡ�ļ��ɣ�*/
		if (!isProcess) {
			//����һ��hadoop�ļ�������
			FSDataInputStream fis = null;
			try {
				//ͨ�������ļ���ȡ�ļ�ϵͳ
				// ���ļ����崦����ȡ
				FileSystem fs = FileSystem.get(configuration);
				//ͨ����Ƭ��Ϣ��ȡ��Ƭ��·��
				// ��ȡ��Ƭ��·��
				Path path = split.getPath();
				//ͨ���ļ�ϵͳ����Ƭ��·����ȡһ��������
				// ��ȡ����Ƭ��������
				fis = fs.open(path);
				//����һ����Ƭ��С�Ļ�����
				byte[] buf = new byte[(int) split.getLength()];
				
				// ��ȡ����
				//nio�ķ�ʽ��ȡ�������е�����
				IOUtils.readFully(fis, buf, 0, buf.length);
				
				// �������
				//���������е�����--�ļ����ݸ�ֵ
				value.set(buf, 0, buf.length);
				
			} finally {
				IOUtils.closeStream(fis);
			}
			
			isProcess = true;
			
			return true;
		}
		
		return false;
	}
    //mrִ�й�����û��key
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
