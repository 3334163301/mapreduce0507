package com.atguigu.mapreduce.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
/**
 * Created by Administrator on 2019/6/13.
 */
public class TestCompress {
    public static void main(String[] args) throws Exception {
        //compress("e:/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
        //compress("e:/hello.txt","org.apache.hadoop.io.compress.GzipCodec");
        //compress("e:/hello.txt","org.apache.hadoop.io.compress.DefaultCodec");
        //decompress("e:/hello.txt.gz");
        decompress("e:/hello.txt.deflate");
    }
    //开始进行压缩
    @SuppressWarnings({"resource","unchecked"})
    private static void compress(String filename, String method) throws Exception {
// 1 获取输入流--这是一个普通java的输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        Class codecClass = Class.forName(method);
        //反射工具类获得的压缩编解码器默认是Object类型，必须通过hadoop的压缩编解码器进行强制类型转换
        CompressionCodec codec = (CompressionCodec)
                ReflectionUtils.newInstance(codecClass, new Configuration());
/*/ 2 获取输出流--这是一个普通java的输出流，
不能达到输出压缩文件的目的；输出的文件名称要拼接该压缩编解码器的默认拓展名*/
        FileOutputStream fos = new FileOutputStream(new File(filename
                +codec.getDefaultExtension()));
        /*通过压缩编解码器的输出流方法包装普通输出流创建压缩输出流*/
        CompressionOutputStream cos = codec.createOutputStream(fos);
// 3 流的对拷
        /*将读到的数据赋值给压缩输出流，并指定一个5M的缓冲区；
        * false表示不允许工具类的copyBytes方法自动关闭流，必须开发人员自己指定手动关闭流，防止数据拷贝不完整*/
        IOUtils.copyBytes(fis, cos, 1024*1024*5, false);
// 4 关闭资源
        fis.close();
        cos.close();
        fos.close();
    }
    // 解压缩
    private static void decompress(String filename) throws FileNotFoundException,
            IOException {
// 0 校验是否能解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new
                Configuration());
        /*根据文件路径获取文件的相关属性详细信息，匹配对应文件类型的编解码器*/
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null) {
            /*如果编解码器为空，直接return--解码失败*/
            System.out.println("cannot find codec for file " + filename);
            return;
        }
// 1 获取输入流---解码器包装一个普通的java输入流创建一个解压缩输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new
                File(filename)));
// 2 获取输出流---实例化一个普通的java输出流--输出解压后的文件
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decoded.txt"));
// 3 流的对拷
         /*将读到的解压缩后数据赋值给普通的java输出流，并指定一个5M的缓冲区；
        * false表示不允许工具类的copyBytes方法自动关闭流，必须开发人员自己指定手动关闭流，防止数据拷贝不完整*/
        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);
// 4 关闭资源
        cis.close();
        fos.close();
    }


}
