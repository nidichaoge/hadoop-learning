package com.mouse.avro.learning.colorcount;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author mouse
 * @date 2020/8/12 12:22
 * @description
 */
public class GenerateUserData {

    public static void main(String[] args) throws IOException {
        new GenerateUserData().serialize();
    }

    public void serialize() throws IOException {
        User user1 = new User();
        user1.setName("mouse");
        user1.setFavoriteNumber(256);
        user1.setFavoriteColor("red");

        User user2 = new User("chao", 7, "red");

        User user3 = User.newBuilder()
                .setName("mousechao")
                .setFavoriteColor("blue")
                .setFavoriteNumber(256)
                .build();

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("./users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

    public void deserialize() throws IOException {
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("./users.avro"), userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

}
