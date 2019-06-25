package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

// ����������
public class WordcountDriver {

	public static void main(String[] args) throws Exception {

		// 1 ��ȡjob������Ϣ
		Configuration configuration = new Configuration();
		// ���� map �����ѹ��
//		configuration.setBoolean("mapreduce.map.output.compress", true);
// ���� map �����ѹ����ʽ
//		configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class,
//				CompressionCodec.class);
//		configuration.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class,
//				CompressionCodec.class);
		Job job = Job.getInstance(configuration);

		// 2 ���ü���jarλ��
		job.setJarByClass(WordcountDriver.class);

		// 3 ����mapper��reducer��class��
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 4 �������mapper����������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 ���������������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// ����С�ļ�
//		job.setInputFormatClass(CombineTextInputFormat.class);
//		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
		
		// 6 �����������ݺ��������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 ���ӷ���
//		job.setPartitionerClass(WordCountPartitioner.class);
//		job.setNumReduceTasks(2);
		
		// 9 ����Combiner
//		job.setCombinerClass(WordCountCombiner.class);

		// ���� reduce �����ѹ������
		FileOutputFormat.setCompressOutput(job, true);
// ����ѹ���ķ�ʽ
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
// FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		
		// 7 �ύ
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}