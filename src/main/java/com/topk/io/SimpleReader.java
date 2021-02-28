package com.topk.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SimpleReader {

    public static List<Set<String>> read(String file) {
        List<Set<String>> frames = new ArrayList<>();
        try (
                FileReader fileReader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            bufferedReader.lines().forEach(i-> frames.add(new HashSet<>(
                    Arrays.stream(i.split(",")).map(x-> x.trim()).collect(Collectors.toList()))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return frames;
    }

    public static void main(String[] args) {
        List<Set<String>> frames = read("data/topk-test/t1.txt");
        for (Set<String> f : frames) System.out.println(f);
    }
}
