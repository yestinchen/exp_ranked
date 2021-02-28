package com.interval.algorithms;

import com.interval.bean.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class UtilsTest {

    @Test
    public void testComputeIntervalSimple1() {
        List<ObjectInterval> list = Arrays.asList(
                new ObjectInterval("A", Arrays.asList(
                        new Interval(1, 6)
                )),
                new ObjectInterval("B", Arrays.asList(
                        new Interval(2, 5)
                ))
        );

        ObjSetInterval result = Utils.computeIntervalByMerging(
                list.stream().map(ObjectInterval::toObjectIntervalSE).collect(Collectors.toList())
        ).toObjSetInterval();
        System.out.println("result:"+result);
        Assert.assertEquals(new HashSet<>(Arrays.asList("A", "B")), result.getObjs());
        Assert.assertEquals(1, result.getIntervals().size());
    }


    @Test
    public void testComputeIntervalSimple2() {
        List<ObjectInterval> list = Arrays.asList(
                new ObjectInterval("A", Arrays.asList(
                        new Interval(590,593)
                )),
                new ObjectInterval("B", Arrays.asList(
                        new Interval(2, 5)
                ))
        );

        ObjSetInterval result = Utils.computeIntervalByMerging(
                list.stream().map(ObjectInterval::toObjectIntervalSE).collect(Collectors.toList())
        ).toObjSetInterval();
        System.out.println("result:"+result);
    }

    @Test
    public void testIntervalUnion() {
        System.out.println(Utils.intervalUnion(
                Arrays.asList(new SETuple(1, TupleType.S), new SETuple(5, TupleType.E)),
                Arrays.asList(new SETuple(2, TupleType.S), new SETuple(6, TupleType.E))
        ));
        System.out.println(Utils.intervalUnion(
                Arrays.asList(new SETuple(1, TupleType.S), new SETuple(4, TupleType.E)),
                Arrays.asList(new SETuple(5, TupleType.S), new SETuple(6, TupleType.E))
        ));
        System.out.println(Utils.intervalUnion(
                Arrays.asList(new SETuple(1, TupleType.S), new SETuple(9, TupleType.E)),
                Arrays.asList(new SETuple(3, TupleType.S), new SETuple(6, TupleType.E))
        ));
    }

    @Test
    public void testFillIntervalsWithSuperset1() {
        ObjSetIntervalSE resultSe = new ObjSetIntervalSE(new HashSet<>(), new ArrayList<>());
        ObjSetIntervalSE superSe = new ObjSetIntervalSE(new HashSet<>(), new ArrayList<>());
        superSe.getTuples().addAll(Arrays.asList(
                new SETuple(1, TupleType.S),
                new SETuple(4, TupleType.E),
                new SETuple(6, TupleType.S),
                new SETuple(8, TupleType.E)
        ));
        Utils.fillIntervalWithSuperSet(resultSe, superSe);
        System.out.println(resultSe.getTuples());
    }

    @Test
    public void testFillIntervalsWithSuperset2() {
        ObjSetIntervalSE resultSe = new ObjSetIntervalSE(new HashSet<>(), new ArrayList<>());
        ObjSetIntervalSE superSe = new ObjSetIntervalSE(new HashSet<>(), new ArrayList<>());
        superSe.getTuples().addAll(Arrays.asList(
                new SETuple(1, TupleType.S),
                new SETuple(4, TupleType.E),
                new SETuple(6, TupleType.S),
                new SETuple(8, TupleType.E)
        ));
        resultSe.getTuples().addAll(Arrays.asList(
                new SETuple(1, TupleType.S),
                new SETuple(4, TupleType.E)
        ));
        Utils.fillIntervalWithSuperSet(resultSe, superSe);
        System.out.println(resultSe.getTuples());
    }

    @Test
    public void testCompactSortTuples() {
        List<SETuple> tuples = new ArrayList<>(Arrays.asList(
                new SETuple(1, TupleType.S),
                new SETuple(1, TupleType.E),
                new SETuple(2, TupleType.S),
                new SETuple(5, TupleType.E),
                new SETuple(9, TupleType.S),
                new SETuple(10, TupleType.E)
        ));
        Utils.compactSortedTuples(tuples);
        System.out.println(tuples);
    }

    @Test
    public void testCompactSortTuples2() {
        List<SETuple> tuples = new ArrayList<>(Arrays.asList(
                new SETuple(231, TupleType.S),
                new SETuple(231, TupleType.E),
                new SETuple(232, TupleType.S),
                new SETuple(235, TupleType.E)
        ));
        Utils.compactSortedTuples(tuples);
        System.out.println(tuples);
    }

    @Test
    public void testCompactSortTuples3() {
        List<SETuple> tuples = new ArrayList<>(Arrays.asList(
                new SETuple(231, TupleType.S),
                new SETuple(231, TupleType.E),
                new SETuple(232, TupleType.S),
                new SETuple(232, TupleType.S),
                new SETuple(232, TupleType.E),
                new SETuple(234, TupleType.E),
                new SETuple(235, TupleType.S),
                new SETuple(235, TupleType.E)
        ));
        Utils.compactSortedTuples(tuples);
        System.out.println(tuples);
    }
}

