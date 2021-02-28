package com.topk.online.result;

import com.topk.bean.Interval;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.*;

public class TopkBookKeeperBreakTie {

    Logger LOG = LogManager.getLogger(TopkBookKeeperBreakTie.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    List<WindowWithScore> bound = new ArrayList<>();
    int k;
    int min = Integer.MIN_VALUE;
    Map<Integer, WindowWithScore> windowSet = new HashMap<>();

    public TopkBookKeeperBreakTie(int k) {
        this.k = k;
    }


    public void update(WindowWithScore window) {
        LOG.debug("updating window: {}, with score: {}", window.getWindow().getStart(), window.getScore());
//        if (window.getWindow().getStart() == 3358){
//            System.out.println("3358:"+window);
//        }
        if (window.getScore() >= min) {
            WindowWithScore existingOne = windowSet.get(window.getWindow().getStart());
            if(existingOne == null) {
                // add
                bound.add(window);
                windowSet.put(window.getWindow().getStart(), window);
            } else if (existingOne.getScore() < window.getScore()) {
                bound.remove(existingOne);
                windowSet.put(window.getWindow().getStart(), window);
                bound.add(window);
            }
            // sort.
            Collections.sort(bound, (x1, x2) -> - Integer.compare(x1.getScore(), x2.getScore()));
            if (bound.size() > k) {
                bound.remove(bound.size() - 1);
            }
            if (bound.size() == k) {
                min = bound.get(bound.size() - 1).getScore();
            }
        }
    }

    public int getMin() {
        if (bound.size() < k) return Integer.MIN_VALUE;
        return min;
    }

    public Collection<WindowWithScore> getTopkResults() {
        return bound;
    }

    public static void main(String[] args) {
        TopkBookKeeperBreakTie topkBookKeeperBreakTie = new TopkBookKeeperBreakTie(2);
        WindowWithScore wws = new WindowWithScore();
        wws.setWindow(new Interval(2,5));
        wws.setScore(3);

        topkBookKeeperBreakTie.update(wws);

        wws = new WindowWithScore();
        wws.setWindow(new Interval(2,5));
        wws.setScore(2);
        topkBookKeeperBreakTie.update(wws);
        System.out.println(topkBookKeeperBreakTie.getTopkResults());
    }
}
