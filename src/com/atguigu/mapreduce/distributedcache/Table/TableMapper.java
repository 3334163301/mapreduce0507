package com.atguigu.mapreduce.distributedcache.Table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//读取某个目录下的所有文件--进行map计算
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{
	TableBean bean = new TableBean();
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
//		1 获取输入文件类型
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		//根据文件路径获取文件名称
		String name = inputSplit.getPath().getName();
		
//		2 获取该文件中输入的每一行数据
		String line = value.toString();
		
//		3 不同文件分别处理
		if (name.startsWith("order")) {// 订单相关信息处理
			// 切割 1001 01 1--对应的是订单编号、产品编号、订单数量
			String[] fields = line.split("\t");
			
			// 封装bean对象 1001	01	1
			bean.setOrder_id(fields[0]);
			bean.setP_id(fields[1]);
			bean.setAmount(Integer.parseInt(fields[2]));
			
			bean.setPname("");
			bean.setFlag("0");//标识这条记录是属于订单表的数据
			
			// 设置key值---两张表都是以产品id为key
			k.set(fields[1]);
			
		}else {// 产品表信息处理     01	小米

			// 切割
			String[] fields = line.split("\t");
			
			// 封装bean对象
			bean.setOrder_id("");
			bean.setP_id(fields[0]);
			bean.setAmount(0);
			bean.setPname(fields[1]);
			bean.setFlag("1");//标识这条记录是属于产品表的数据
			
			// 设置key值---两张表都是以产品id为key
			k.set(fields[0]);
		}
		
//		4 封装bean对象输出
		context.write(k, bean);
	}

}
