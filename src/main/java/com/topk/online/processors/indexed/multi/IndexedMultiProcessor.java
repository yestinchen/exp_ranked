package com.topk.online.processors.indexed.multi;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.component.MPWGenWGroup;
import com.topk.online.processors.ConditionItem;
import com.topk.online.result.WindowWithScore;
import com.topk.online.retriever.EarlyStopperNever;
import com.topk.online.retriever.KeyMapperDummy;
import com.topk.online.retriever.NodeAssertionStr;
import com.topk.online.retriever.SimpleRootNodeExtractor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Process multiple types.
 */
public class IndexedMultiProcessor {

    Logger LOG = LogManager.getLogger(IndexedMultiProcessor.class);

    {
        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    Map<CLabel, List<BasePartition<Node<String, PayloadIntervals>, IndexedPartitionPayload>>> partitionMap;

    int partitionSize;

    public IndexedMultiProcessor(Map<CLabel,
            List<BasePartition<Node<String, PayloadIntervals>, IndexedPartitionPayload>>> partitionMap, int partitionSize) {
        this.partitionMap = partitionMap;
        this.partitionSize = partitionSize;
    }

    public Collection<WindowWithScore> topk(int k, int w, List<List<ConditionItem>> conditions) {
        int partitionNum = (int) Math.ceil((w)*1.0/partitionSize);
        int maxPartitionNum = (int) Math.ceil((w-1)*1.0/partitionSize)+1;

        List<MultiPartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>,
                String, IndexedPartitionPayload>> multiPartitionWindows =
                MPWGenWGroup.genMPWs(partitionMap, conditions, partitionNum, partitionSize,
                        (selectedObjs, x)-> new NodeAssertionStr(selectedObjs), new SimpleRootNodeExtractor<>(),
                        (s, x) ->new KeyMapperDummy<>(), (s, x)-> new EarlyStopperNever<>());
        // create working partitions.
        return null;
    }
}
