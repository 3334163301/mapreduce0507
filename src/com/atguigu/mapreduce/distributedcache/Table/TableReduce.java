package com.atguigu.mapreduce.distributedcache.Table;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {
    //ÿ�ν�����key---pid��Ψһ�ģ�value����������bean�����pid������ͬ�ģ���ͬpid��order bean�����ж��--����prod bean����ֻ��һ��
	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context)
			throws IOException, InterruptedException {
		
		// 0 ׼���洢���ݵĻ���--����������ڴ�Ų�Ʒ���е�ÿһ����¼
		TableBean pdbean = new TableBean();
		//���������ŵ������ж������еļ�¼
		ArrayList<TableBean> orderBeans = new ArrayList<>();

		// �����ļ��Ĳ�ͬ�ֱ�������

		for (TableBean bean : values) {

			if ("0".equals(bean.getFlag())) {// ���������ݴ���
				// 1001 1
				// 1001 1
				TableBean orBean = new TableBean();//����������������翵�ֵ���ص�һ��ʵ����������

				try {
					//�Ѷ�������ÿ����¼��Ӧ������ֵ�����������涩��bean������
					BeanUtils.copyProperties(orBean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
               //���������bean������ӽ�����
				orderBeans.add(orBean);
				/*�������е�ֵ��������翵ģ���ӽ����ϣ������߳�����ʱ--��ֵ�ڼ�������ʧ*/
//				orderBeans.add(bean);

			} else {// ��Ʒ���� 01 С��
				try {
					//����������¼�����ڲ�Ʒ���Ѳ�Ʒ����ÿ����¼��Ӧ������ֵ�������������Ʒbean������
					BeanUtils.copyProperties(pdbean, bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		// ����ƴ��
		//����������
		for (TableBean bean : orderBeans) {
			// ���²�Ʒ�����ֶ�
			//ÿһ��reduce�������������bean�����pid������ͬ��
			bean.setPname(pdbean.getPname());
			
			// д��
			context.write(bean, NullWritable.get());
		}
	}
}
