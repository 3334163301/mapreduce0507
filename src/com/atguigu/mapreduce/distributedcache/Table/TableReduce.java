package com.atguigu.mapreduce.distributedcache.Table;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {
    //每次进来的key---pid是唯一的，value集合中所有bean对象的pid都是相同的，相同pid的order bean对象有多个--但是prod bean对象只有一个
	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context)
			throws IOException, InterruptedException {
		
		// 0 准备存储数据的缓存--这个对象用于存放产品表中的每一条记录
		TableBean pdbean = new TableBean();
		//集合里面存放的是所有订单表中的记录
		ArrayList<TableBean> orderBeans = new ArrayList<>();

		// 根据文件的不同分别处理数据

		for (TableBean bean : values) {

			if ("0".equals(bean.getFlag())) {// 订单表数据处理
				// 1001 1
				// 1001 1
				TableBean orBean = new TableBean();//将迭代器中虚无缥缈的值承载到一个实例化对象中

				try {
					//把订单表中每条记录对应的属性值，拷贝到缓存订单bean对象当中
					BeanUtils.copyProperties(orBean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
               //将订单表的bean对象添加进集合
				orderBeans.add(orBean);
				/*迭代器中的值是虚无缥缈的，添加进集合，程序走出集合时--该值在集合中消失*/
//				orderBeans.add(bean);

			} else {// 产品表处理 01 小米
				try {
					//否则这条记录就属于产品表，把产品表中每条记录对应的属性值，拷贝到缓存产品bean对象当中
					BeanUtils.copyProperties(pdbean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		// 数据拼接
		//遍历订单表
		for (TableBean bean : orderBeans) {
			// 更新产品名称字段
			//每一次reduce方法处理的所有bean对象的pid都是相同的
			bean.setPname(pdbean.getPname());
			
			// 写出
			context.write(bean, NullWritable.get());
		}
	}
}
