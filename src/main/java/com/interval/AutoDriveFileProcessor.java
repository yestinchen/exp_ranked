package com.interval;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AutoDriveFileProcessor {

    public static void transformFile(String filePath, String outputPath) throws IOException {
        System.out.println("processing:"+filePath);
        System.out.println("to:"+outputPath);
        JSONArray array = new JSONArray(Files.readString(Paths.get(filePath)));
        StringBuilder sb = new StringBuilder();
        int lastId = -1;
        for (int i =0; i < array.length(); i++) {
            JSONObject object = array.getJSONObject(i);
            int index = object.getInt("index");
            while(index < lastId+1) {
                lastId ++;
                sb.append("\n");
            }
            JSONArray labels = object.getJSONArray("labels");
            Map<String, List<String>> typeIdMap = new HashMap<>();
            for (int j =0 ; j < labels.length(); j++) {
                JSONObject objInfo = labels.getJSONObject(j);
                String id = objInfo.getString("id");
                String type  = objInfo.getString("category");
//                System.out.println(id+";"+type);
                typeIdMap.computeIfAbsent(type, x -> new ArrayList<>()).add(id);
            }
            for (String type: typeIdMap.keySet()) {
                sb.append(type).append(":<")
                        .append(String.join(",", typeIdMap.get(type)))
                .append(">;");
            }
            sb.append("\n");
        }
        Files.writeString(Paths.get(outputPath), sb);
    }

    public static void processFiles(String filePath, String outputFile) throws IOException {
        Files.list(Paths.get(filePath)).sorted()
                .forEach(i -> {
                    try {
                        transformFile(i.toAbsolutePath().toString(),
                                outputFile+"/"+i.getFileName().toString().replace(".json", ".txt"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public static void main(String[] args) throws IOException {
//        listFile("/media/ytchen/hdd/dataset/autodrive/bdd100k_labels20_box_track/bdd100k/labels-20/box-track/train");
//        readFile("/media/ytchen/hdd/dataset/autodrive/bdd100k_labels20_box_track/bdd100k/labels-20/box-track/train/0000f77c-6257be58.json");
        processFiles("/media/ytchen/hdd/dataset/autodrive/bdd100k_labels20_box_track/bdd100k/labels-20/box-track/train",
                "/media/ytchen/hdd/dataset/autodrive/bdd100k_labels20_box_track/bdd100k/labels-20/box-track/train-seq");
    }
}
