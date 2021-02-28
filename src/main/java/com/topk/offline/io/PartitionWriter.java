package com.topk.offline.io;

import com.common.bean.Tuple2;
import com.common.io.SimplifiedIOs;
import com.topk.offline.SingleIndexBuilder;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PartitionWriter {

    public static <T, Q> List<Tuple2<Long, Integer>> writePartitionData(List<BasePartition<T, Q>> partitions, String path) throws IOException {
        List<Tuple2<Long, Integer>> pStartPositions = new ArrayList<>();
        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
        FileChannel fileChannel= randomAccessFile.getChannel();

        int currentPos = 0;
        for (BasePartition<T, Q> p : partitions) {
            long pos = fileChannel.position();
            // 1. write length
            // write partition.
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);


            objectOutputStream.writeObject(p.getRoots());

            byte[] source = byteArrayOutputStream.toByteArray();
            ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, currentPos, source.length);
            buffer.put(source);

            // reset
            buffer.clear();
            byteArrayOutputStream.reset();
            objectOutputStream.reset();

            pStartPositions.add(new Tuple2<>(pos, source.length));

            currentPos += source.length;
        }
        fileChannel.close();
        randomAccessFile.close();

        return pStartPositions;
    }

    public static <T, Q> void writePartitionMeta(List<BasePartition<T, Q>> partitions, List<Tuple2<Long, Integer>> posList, String path) throws IOException {
        // handle position list.

        RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw");
        FileChannel fileChannel= randomAccessFile.getChannel();

        int currentPos = 0;
        for (int i =0; i < partitions.size(); i++) {
            // remember.
            BasePartition<T, Q> partition = partitions.get(i);
            Tuple2<Long, Integer> tuple = posList.get(i);
            // write
            PMetaObj obj = new PMetaObj();
            obj.setBlockSize(tuple.get_2());
            obj.setStartPos(tuple.get_1());
            obj.setStartFrame(partition.getStartFrame());
            obj.setObjs(partition.getObjs());
            obj.setSize(partition.getSize());
            obj.setTop1Map(partition.getTop1Map());

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            objectOutputStream.writeObject(obj);

            byte[] source = byteArrayOutputStream.toByteArray();

            ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, currentPos, source.length + 4);

            buffer.putInt(source.length);

            buffer.put(source);

            // reset
            buffer.clear();
            byteArrayOutputStream.reset();
            objectOutputStream.reset();

            currentPos += source.length + 4;
        }
    }

    public static <T, Q> void writePartitions(List<BasePartition<T, Q>> partitions, String basePath) throws IOException {
        List<Tuple2<Long, Integer>> tupleList = writePartitionData(partitions, basePath+"/"+Constants.P_DATA_FILE);
        writePartitionMeta(partitions, tupleList, basePath+"/"+Constants.P_META_FILE);
    }

    public static void main(String[] args) throws IOException {
        String file = "data/new/visualroad1.txt";
        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, Arrays.asList("person"));
        int partitionSize = 600;

        SingleIndexBuilder indexBuilder = new SingleIndexBuilder();

        List<BasePartition<Node<String, PayloadIntervals>, Byte>> partitions = indexBuilder.build(frames, partitionSize);

        String basePath="testrw";
        if (!new File(basePath).exists()) {
            new File(basePath).mkdirs();
        }
        writePartitions(partitions, basePath);
    }
}
