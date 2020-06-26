package Evaluation;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class RecallStream {

    public DataStream<Integer> recallStream(DataStream<Tuple3<Integer,String,ArrayList<String>>> itemRecommendationList){

        DataStream<Integer> recallOutput = itemRecommendationList.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<Integer, String, ArrayList<String>>, Integer>() {
                    @Override
                    public void processElement(Tuple3<Integer, String, ArrayList<String>> input, Context context, Collector<Integer> out) throws Exception {

                        Integer recall = new EvaluationMethods().recallOnline(input.f1,input.f2);
                        out.collect(recall);
                    }
                });

        return recallOutput;
    }

}
