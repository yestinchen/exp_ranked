package com.topk.online.component;

import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.PartitionWindow;
import com.topk.online.PrefixList;
import com.topk.online.processors.ConditionItem;

import java.util.*;

public class MPWMapRetriever {

    public static List<List<Map<Set<String>, PrefixList<String>>>> retrievePrefixMap(
            MultiPartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String, Byte> mpw,
            List<List<ConditionItem>> conditions) {
        // type -> partitions -> map.
        List<List<Map<Set<String>, PrefixList<String>>>> prefixMapList = new ArrayList<>();
        for (List<ConditionItem> cil : conditions) {
            // and op.
            List<Map<Set<String>, PrefixList<String>>> prefixMaps = null;
            for (ConditionItem ci: cil) {
                if (!mpw.getPwMap().containsKey(ci.getType())) continue;
                // or op.
                PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String, Byte> pwi = mpw.getPwMap().get(ci.getType());
                List<Map<Set<String>, PrefixList<String>>> singlePrefixMap = pwi.getPrefixMaps();
                // create
                if (prefixMaps == null) {
                    prefixMaps = new ArrayList<>();
                    for (int i=0; i < singlePrefixMap.size(); i++) {
                        prefixMaps.add(new HashMap<>());
                    }
                }
                // put & merge every object.
                for (int i =0; i < singlePrefixMap.size(); i++) {
                    prefixMaps.get(i).putAll(singlePrefixMap.get(i));
                }
            }
            // add to list.
            if (prefixMaps != null) {
                prefixMapList.add(prefixMaps);
            }
        }
        return prefixMapList;
    }
}
