package com.mouse.hadoop.io.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 通过CompressionCodec对数据流进行压缩和解压缩
 * 从标准输入读入，写出到标准输出
 */
public class StreamCompressor {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        String codeClassName = args[0];
        Class<?> codeClass = Class.forName(codeClassName);
        Configuration configuration = new Configuration();
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codeClass,configuration);
        CompressionOutputStream outputStream = compressionCodec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in,outputStream,4096,false);
        outputStream.finish();
    }

}
