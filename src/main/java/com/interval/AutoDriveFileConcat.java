package com.interval;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;

public class AutoDriveFileConcat {

    public static void list(String file) throws IOException {
        List<Path> files = Files.list(Paths.get(file)).sorted().collect(Collectors.toList());
        System.out.println("size:"+files.size());
    }

    public static void concat(String file, int start, int end, String output) throws IOException {
        List<Path> files = Files.list(Paths.get(file)).sorted().collect(Collectors.toList());

        boolean first = true;
        for (Path p : files.subList(start, end)) {
            String read = Files.readString(p);
            Files.writeString(Paths.get(output), read, first ? StandardOpenOption.CREATE_NEW : StandardOpenOption.APPEND);
            first = false;
        }
    }
    public static void main(String[] args) throws IOException {
//        list("/media/ytchen/hdd/dataset/autodrive/bdd100k_labels20_box_track/bdd100k/labels-20/box-track/train-seq/");
        String folder = "/media/ytchen/hdd/dataset/autodrive/bdd100k_labels20_box_track/bdd100k/labels-20/box-track/train-seq/";
        concat(folder, 0, 400, "./data/concat/d1.txt");
        concat(folder, 400, 800, "./data/concat/d2.txt");
        concat(folder, 800, 1200, "./data/concat/d3.txt");
    }
}
