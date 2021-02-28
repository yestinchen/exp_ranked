package com.topk.offline.bean;

import com.topk.offline.io.PMetaObj;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class LazyPartition<T, Q> extends BasePartition<T, Q> {

    private static final long serialVersionUID = -7163921412629428098L;

    long startPos;
    int blockSize;

    public LazyPartition() {

    }

    public LazyPartition(PMetaObj obj) {
        this.startPos = obj.getStartPos();
        this.blockSize = obj.getBlockSize();
        this.startFrame = obj.getStartFrame();
        this.size = obj.getSize();
        this.objs = obj.getObjs();
        this.top1Map = obj.getTop1Map();
    }

    public void fetchFromDisk(FileChannel fileChannel) throws IOException, ClassNotFoundException {
        if (blockSize <= 0) return;
        fileChannel.position(startPos);
        // read.
        ByteBuffer content = ByteBuffer.allocate(blockSize);
        fileChannel.read(content);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content.array());
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

        roots = List.class.cast(objectInputStream.readObject());
        objectInputStream.close();
        byteArrayInputStream.close();
    }
}
