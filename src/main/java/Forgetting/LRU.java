package Forgetting;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class LRU{
    public DataStream<Tuple3<Integer,String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingWay) {

        DataStream<Tuple3<Integer, String, Map<String, Float>>> estimatedRateUserItemMap = withKeyStream.keyBy(0)
                .process(new LRUForgetting());

        return estimatedRateUserItemMap;
    }
}

