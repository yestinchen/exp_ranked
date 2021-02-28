package com.common.io;

import com.common.bean.VideoFrame;
import com.common.bean.VideoSequence;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class VideoSequenceIO {

    public static VideoSequence readFromFile(String file, List<String> objTypes) {
        VideoSequence videoSequence = new VideoSequence();
        try (
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                ) {
            bufferedReader.lines().forEach(i -> {
                i = i.trim();
                if (i.equals("")) {
                    // empty.
                    videoSequence.addFrame(new VideoFrame());
                } else {
                    // split ;
                    VideoFrame videoFrame = new VideoFrame();
                    String groups[] = i.split(";");
                    for (String group: groups) {
                        String kv[] = group.split(":");
                        String type = kv[0].trim();
                        if (objTypes.contains(type)) {
                            // add all values.
                            String idstr = kv[1].trim();
                            String ids[] = idstr.substring(1, idstr.length()-1).split(",");
                            for (String id: ids) {
                                id = id.trim();
                                if (id.equals("")) continue; // ignore empty str.
                                videoFrame.add(id);
                                if (!videoSequence.hasIdType(id)) {
                                    videoSequence.addIdMapping(id, type);
                                }
                            }
                        }
                    }
                    videoSequence.addFrame(videoFrame);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
        return videoSequence;
    }

    public static void main(String[] args) {
        VideoSequence videoSequence = readFromFile("data/new/visualroad1.txt", Arrays.asList("person"));
        System.out.println(videoSequence);
    }
}
