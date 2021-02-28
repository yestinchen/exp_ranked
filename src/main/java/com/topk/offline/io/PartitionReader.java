package com.topk.offline.io;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.LazyPartition;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class PartitionReader {

    public static <T, Q> List<BasePartition<T, Q>> readLazyPartitions(String basePath) throws IOException, ClassNotFoundException {
        List<BasePartition<T, Q>> lazyPartitions = new ArrayList<>();
        for (PMetaObj obj: readPartitionMetas(basePath+"/"+Constants.P_META_FILE)) {
            lazyPartitions.add(new LazyPartition<>(obj));
        }
        return lazyPartitions;
    }

    public static RandomAccessFile openPartitionFile(String basePath) throws FileNotFoundException {
        RandomAccessFile accessFile = new RandomAccessFile(basePath+"/"+Constants.P_DATA_FILE, "r");
        return accessFile;
    }

    public static List<PMetaObj> readPartitionMetas(String file) throws IOException, ClassNotFoundException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        FileChannel fileChannel = randomAccessFile.getChannel();

        // read all.
        ByteBuffer byteBuffer = ByteBuffer.allocate(4); // for length.
        List<PMetaObj> pMetaObjs = new ArrayList<>();
        while(fileChannel.position() < fileChannel.size()) {
            int read = fileChannel.read(byteBuffer);
            byteBuffer.flip();
            int size = byteBuffer.getInt();
            ByteBuffer content = ByteBuffer.allocate(size);
            fileChannel.read(content);

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content.array());
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            PMetaObj obj = PMetaObj.class.cast(objectInputStream.readObject());
            pMetaObjs.add(obj);

            byteBuffer.clear();
            byteArrayInputStream.close();
            objectInputStream.close();
        }

        fileChannel.close();
        randomAccessFile.close();

        return pMetaObjs;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        List<PMetaObj> metaObjs = readPartitionMetas("testrw/" + Constants.P_META_FILE);
        for (PMetaObj obj: metaObjs) {
            System.out.println(obj);
        }
    }
}
