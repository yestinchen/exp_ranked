package com.interval;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class GenIntervalFile {

    static Map<String, String> map = new HashMap<>();
    static {
        map.put("0", "person");
        map.put("2", "car");
        map.put("5", "bus");
        map.put("7","truck");
    }

    public static void genSeq(String input, String output) throws IOException {
        Map<Integer, List<Bean>> frameMap = new HashMap<>();
        AtomicInteger maxId = new AtomicInteger();
        Files.lines(Path.of(input)).filter(i -> !i.startsWith("#")).forEach(i -> {
            String[] arr = i.split(":");
            int fid = Integer.parseInt(arr[0].trim());
            String rest = arr[1].trim();
            rest = rest.substring(1, rest.length() - 1);
            String[] arr2 = rest.split(",");
            String trackId = arr2[4].trim();
            String objType = map.get(arr2[5].trim());
            Bean newBean = new Bean();
            newBean.id = objType+trackId;
            newBean.type = objType;
            frameMap.computeIfAbsent(fid, x -> new ArrayList<>()).add(newBean);
            if (fid > maxId.get()) maxId.set(fid);
        }
        );
        FileWriter fileWriter = new FileWriter(new File(output));
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        for (int i =0; i <=maxId.get(); i++) {
            List<Bean> list = frameMap.getOrDefault(i, Collections.emptyList());
            // build index
            Map<String, List<String>> index = new HashMap<>();
            for (Bean b : list) {
                index.computeIfAbsent(b.type, x -> new ArrayList<>()).add(b.id);
            }
            List<Map.Entry<String, List<String>>> sorted = new ArrayList<>(index.entrySet());
            sorted.sort((x1,x2)->x1.getKey().compareTo(x2.getKey()));
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, List<String>> kv: sorted) {
                sb.append(kv.getKey()).append(":<");
                List<String> ids = new ArrayList<>(new HashSet<>(kv.getValue()));
                Collections.sort(ids);
                sb.append(String.join(",",ids)).append(">;");
            }
            bufferedWriter.write(sb.toString()+"\n");
        }
        bufferedWriter.close();
        fileWriter.close();
    }

    public static void setup(List<String> names) throws IOException {
        for (String name: names) {
            genSeq("/media/ytchen/hdd/working/yolo/"+name+"/tracked.txt", "data/yolo/"+name+".txt");
        }
    }

    public static void main(String[] args) throws IOException {
//        genSeq("/media/ytchen/hdd/working/yolo/MOT16-06/tracked.txt", "data/yolo/MOT16-06.txt");
//        genSeq("/media/ytchen/hdd/working/yolo/MOT16-13/tracked.txt", "data/yolo/MOT16-13.txt");
//        genSeq("/media/ytchen/hdd/working/yolo/MVI_40171/tracked.txt", "data/yolo/MVI_40171.txt");
//        genSeq("/media/ytchen/hdd/working/yolo/MVI_40751/tracked.txt", "data/yolo/MVI_40751.txt");
//        genSeq("/media/ytchen/hdd/working/yolo/traffic1/tracked.txt", "data/yolo/traffic1.txt");
//        genSeq("/media/ytchen/hdd/working/yolo/traffic1/tracked.txt", "data/yolo/traffic1.txt");
//        setup(Arrays.asList("traffic2", "traffic3", "news1", "news2", "news3"));
//        setup(Arrays.asList("joker", "inception", "ff"));
        setup(Arrays.asList("midway"));
    }

    static class Bean {
        public String type;
        public String id;
    }
}
