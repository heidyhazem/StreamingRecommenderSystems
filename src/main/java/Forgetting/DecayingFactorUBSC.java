package Forgetting;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import Matrix.SparseBinaryMatrix;

import java.util.Map;

public class DecayingFactorUBSC{

    public DataStream<Tuple3<Integer,String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k,
                                                                      Integer windowSize, Integer windowSlide){

        DataStream<Tuple3<Integer,String, Map<String, Float>>> estimatedRateUserItemMap = withKeyStream.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<Integer, String, Map<String, Float>>>() {

                    /**
                     holds state for user's rating for items as we need to know which items the user rated to
                     calculate the user's similarities
                     */
                    MapState<Integer, SparseBinaryMatrix> userItemRatingHistoryState;
                    /**
                     holds state for user's similarities parts to be incrementally updated((user,user),countOfCommonItems,countOfRatedItemsU1,countOfRatedItemsU2)
                     */

                    MapState<Integer, SparseBinaryMatrix> userSimilaritiesState;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<Integer, String, Map<String, Float>>> out) throws Exception {

                        String user = input.f1;
                        String item = input.f2;


                        //################################################################################################################################
                        //**********************//Step3: prepare similarities for recommendation before update *******************************************
                        if(userItemRatingHistoryState.get(0).getRowLabels().contains(user)){
                            /**
                             * Updating similarities state
                             */
                            //generate pairs
                            //step 2:1 similarities
                        }





                    }

                    @Override
                    public void open(Configuration config) {
                        MapStateDescriptor<Integer, SparseBinaryMatrix> descriptor1 =
                                new MapStateDescriptor<>(
                                        "userItemRatingHistory",
                                        TypeInformation.of(new TypeHint<Integer>() {

                                        }),
                                        TypeInformation.of(new TypeHint<SparseBinaryMatrix>() {

                                        })
                                );


                        MapStateDescriptor<Integer, SparseBinaryMatrix> descriptor2 =
                                new MapStateDescriptor<>(
                                        "userItemRatingHistory",
                                        TypeInformation.of(new TypeHint<Integer>() {

                                        }),
                                        TypeInformation.of(new TypeHint<SparseBinaryMatrix>() {

                                        })
                                );



                        userItemRatingHistoryState = getRuntimeContext().getMapState(descriptor1);
                        userSimilaritiesState = getRuntimeContext().getMapState(descriptor2);

                    }
                });



        return null;
    }
}