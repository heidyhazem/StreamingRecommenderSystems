import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Iterables;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.IntegerSerializer;
import partitioning.Batch;
import readingSources.SourceWithTimestamp;
import sun.plugin.javascript.navig.Array;

import java.util.HashMap;
import java.util.Map;



//This to test session window
public class test{


    public static void main (String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set streaming execution environment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
        Holds initialization of the inputStream
                */

        DataStream<Tuple3<String, String, Float>> inputStream = null;


        //***************************************Adding source(txt file (user,item,rate,timestamp))********************************
        if (params.has("input") && params.has("records")) {
            inputStream = env.addSource(new SourceWithTimestamp(params.get("input"), params.getInt("records")));
        } else if (params.has("input") && !params.has("records")) {
            inputStream = env.addSource(new SourceWithTimestamp(params.get("input")));
        } else {
            System.out.println("Use --input to specify file input  or use --records to specify records");
        }

        /**
         Generate unified key for all records for batch partitioning
         */
        DataStream<Tuple4<Integer,String,String,Float>> withKeyedStream = new Batch(inputStream).generateOneKey(inputStream);


        DataStream<Tuple2<String,String>> checkStream = withKeyedStream.keyBy(0)
                .countWindow(3,1)
                //.trigger(CountTrigger.of(1))
                .process(new ProcessWindowFunction<Tuple4<Integer, String, String, Float>, Tuple2<String, String>, Tuple, GlobalWindow>() {

                    int[][] x ;

                    MapState<String,Integer> allItems;

                    private final  ValueStateDescriptor<Long> previousFiringState = new ValueStateDescriptor<Long>(
                            "Previous Firing",
                            LongSerializer.INSTANCE

                    );


                    private MapStateDescriptor<String,Integer> allItemsDescriptor = new MapStateDescriptor<>(
                            "allItemsDesc",
                            TypeInformation.of(new TypeHint<String>() {}),
                            TypeInformation.of(new TypeHint<Integer>() {}));


                    private final  ValueStateDescriptor<Float> previousFiringState2 = new ValueStateDescriptor<Float>(
                            "Previous Firing",
                            FloatSerializer.INSTANCE);


                    MapState<String,Map<String,Float>> userItemRatingHistory;




                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple4<Integer, String, String, Float>> iterable, Collector<Tuple2<String, String>> out) throws Exception {

                        int[][] right = new int[2][];



                        MapState<String,Integer> allItemsWindow = context.windowState().getMapState(allItemsDescriptor);

                        ValueState<Float> previousFiring = context.windowState().getState(previousFiringState2);





                        Tuple4<Integer, String, String, Float> output = Iterables.getLast(iterable);
                        Tuple4<Integer, String, String, Float> output2 = Iterables.getFirst(iterable,null);
                        //out.collect(Tuple2.of("first of window","first of window"));
                        //out.collect(Tuple2.of(output2.f1,output2.f2));

                        //out.collect(Tuple2.of("end of window","end of window"));
                        //out.collect(Tuple2.of(output.f1,output.f2));


                       for (Tuple4<Integer, String, String, Float> input : iterable) {
                            allItemsWindow.put(input.f2,1);
                            //out.collect(Tuple2.of(input.f1,input.f2));
                        }

                        for (String items : allItemsWindow.keys()) {
                            //out.collect(Tuple2.of(items,items));

                        }

                        for (Tuple4<Integer, String, String, Float> input : iterable){
                           Map<String,Float> x = new HashMap<>();
                           x.put(input.f2,input.f3);
                           x.put("testItem",55f);
                           userItemRatingHistory.put(input.f1,x);
                           //userItemRatingHistory.get(input.f1).remove("testItem");

                           //x.remove("testItem");
                        }


                        out.collect(Tuple2.of("window","window"));

                        for(Map.Entry<String,Map<String,Float>> i : userItemRatingHistory.entries()){
                           out.collect(Tuple2.of(i.getKey(),"ngrbbbbbbbb"));
                           for(Map.Entry<String,Float> z : i.getValue().entrySet()){
                               out.collect(Tuple2.of(z.getKey(),z.getKey()));
                           }
                        }




                        allItemsWindow.clear();
                        userItemRatingHistory.clear();


                        //*********************************************************************************************









                    }

                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Integer> descriptor3 =
                                new MapStateDescriptor<>(
                                        "allItems",
                                        TypeInformation.of(new TypeHint<String>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Integer>() {

                                        })
                                );



                        MapStateDescriptor<String, Map<String, Float>> descriptor4 =
                                new MapStateDescriptor<>(
                                        "userItemRatingHistory",
                                        TypeInformation.of(new TypeHint<String>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Map<String, Float>>() {

                                        })
                                );

                        userItemRatingHistory = getRuntimeContext().getMapState(descriptor4);

                        allItems = getRuntimeContext().getMapState(descriptor3);
                    }

                    @Override
                    public void clear (Context context){
                        ValueState<Long> previousFiring = context.windowState().getState(previousFiringState);
                        MapState<String,Integer> allItemsWindow = context.windowState().getMapState(allItemsDescriptor);

                        previousFiring.clear();
                        allItemsWindow.clear();

                    }





                });





        if(params.has("output")){
            checkStream.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Use --output to specify file input ");
        }

        env.execute("TenscentRec central");



        }



    }
