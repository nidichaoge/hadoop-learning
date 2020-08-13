package com.mouse.hadoop.io.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

/**
 *
 */
public class SequenceFileWriteDemo {

    private static final String[] DATE = {
            "",
            "",
            "",
            "",
            "",
    };

    public static void main(String[] args) throws IOException {
        String uri =  args[0];
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri), configuration);
        Path path = new Path(uri);

        IntWritable intWritable = new IntWritable();
        Text text = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fileSystem,configuration,path,intWritable.getClass(),text.getClass());
            for (int i =0;i<100;i++){
                intWritable.set(100-i);
                text.set(DATE[i%DATE.length]);
                System.out.printf("[%s]\t%s\t%s\n",writer.getLength(),intWritable,text);
                writer.append(intWritable,text);
            }
        }finally {
            IOUtils.closeStream(writer);
        }
    }

}
