package com.common.io;

import com.common.bean.Tuple2;
import com.common.bean.VideoFrame;
import com.common.bean.VideoSequence;
import com.topk.offline.bean.CLabel;

import java.util.*;

public class SimplifiedIOs {

    public static List<Set<String>> readIDOnly(String file, List<String> types) {
        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, types);
        List<Set<String>> frames = new ArrayList<>();
        for (VideoFrame f : videoSequence.getFrames()) {
            frames.add(f.getIds());
        }
        return frames;
    }

    public static Map<String, List<Set<String>>> readIDGroupByType(String file, List<String> types) {
        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, types);

        Map<String, List<Set<String>>> typeFrames = new HashMap<>();
        for (VideoFrame f : videoSequence.getFrames()) {
            Map<String, Set<String>> typeIds = new HashMap<>();
            for (String type: types) {
                Set<String> ids = new HashSet<>();
                typeFrames.computeIfAbsent(type, i -> new ArrayList<>()).add(ids);
                typeIds.put(type, ids);
            }
            for (String id : f.getIds()) {
                String type = videoSequence.getIdToType().get(id);
                typeIds.get(type).add(id);
            }

        }
        return typeFrames;
    }

    public static Tuple2<List<Set<String>>, Map<String, CLabel>> readIDsAndTypeMapping(String file, List<String> types) {
        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, types);

        List<Set<String>> frames = new ArrayList<>();
        for (VideoFrame f : videoSequence.getFrames()) {
            frames.add(f.getIds());
        }

        // translate mapping.
        Map<String, CLabel> typeMapping = new HashMap<>();
        for (Map.Entry<String, String> entry: videoSequence.getIdToType().entrySet()) {
            CLabel label = CLabel.labelFor(entry.getValue());
            if (label == null) {
                System.out.println("untranslated label:" + entry.getValue());
                System.exit(-1);
            }
            typeMapping.put(entry.getKey(), label);
        }
        return new Tuple2<>(frames, typeMapping);
    }
}
