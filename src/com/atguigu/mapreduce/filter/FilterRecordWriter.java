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
	//�����������·���������
	private FSDataOutputStream atguiguOut = null;
	private FSDataOutputStream otherOut = null;
	/*ͨ�����������л��������Ķ�������������*/
	public FilterRecordWriter(TaskAttemptContext job)  {
		/*ͨ�����������л��������Ķ����ȡHadoop������Ϣ*/
		Configuration configuration = job.getConfiguration();
		
		try {
			/*ͨ��Hadoop������Ϣ��ȡ�ļ�ϵͳ��Ϣ*/
			// ��ȡ�ļ�ϵͳ
			FileSystem fs = FileSystem.get(configuration);
			
			// ���������ļ��������
			atguiguOut = fs.create(new Path("e:/output/atguigu.log"));
			/*ָ���������˵����·��λ�ã�ͨ���ļ�ϵͳ������Ӧ·���������*/
			otherOut = fs.create(new Path("e:/output/other.log"));
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// ���������key�Ƿ����atguigu
		
		if (key.toString().contains("atguigu")) {// ����
			/*����ʵ��������ɹ�֮������·������������ᱻ�Զ�������
			* ͨ����Ӧ�����������Ӧ��reduce��������Ŀ��·��*/
			atguiguOut.write(key.toString().getBytes());
		}else {// ������
			otherOut.write(key.toString().getBytes());
		}
	}
    /*ֻ�йر������֮�󣬽���Ż�д��Ŀ��·��*/
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
