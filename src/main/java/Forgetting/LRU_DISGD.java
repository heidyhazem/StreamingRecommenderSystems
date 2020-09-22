package Forgetting;

import Recommender.userItem;
import VectorsOp.VectorOperations;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import IncrementalMatrixFactorization.*;

public class LRU_DISGD{



    public DataStream<Tuple3<String, String, Map<String, Float>>> fit2(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingWay) {


        DataStream<Tuple3<String, String, Map<String, Float>>> itemsScores = withKeyStream.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<String, String, Map<String, Float>>>() {

                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;




                    private ValueState<Long> generalCounter;
                    private ValueState<Boolean> status;
                    private MapState<String, Long> userFootPrint;
                    private MapState<String, Long> itemFootPrint;

                    int latentFeatures = 10;
                    Double lambda = 0.01;
                    Double mu = 0.05;

                    //number of recommended items
                    Integer N = 10;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<String, String, Map<String, Float>>> out) throws Exception {

                        //Matrix factorization for each bag
                        String user = input.f1;
                        String item = input.f2;


                        SGD sgd = new SGD();
                        userItem userItemVectors;

                        Double[] itemVector;
                        Double[] userVector;
                        Boolean knownUser = false;


                        ArrayList<String> recommendedItems;

                        //Map<String,Float> itemsScoresMatrixMap = new HashMap<>();
                        Map<String, Float> itemsScoresMatrixMap = new HashMap<>();


                        //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                        //************************get or initialize user vector************************
                        if (usersMatrix.contains(user)) {
                            userVector = usersMatrix.get(user);
                            knownUser = true;
                        }
                        //user is not known before.
                        //initialize it
                        else {
                            userVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //userVector = VectorOperations.initializeVector(latentFeatures);
                        }
                        //******************************************************************************
                        //************************get or initialize item vector*************************

                        if (itemsMatrix.contains(item)) {
                            itemVector = itemsMatrix.get(item);
                        }
                        //item is not known before.
                        //initialize it
                        else {

                            itemVector = new Double[]{0.04412275, -0.03308702, 0.24307712, -0.02520921, 0.01096098,
                                    0.15824811, -0.09092324, -0.05916367, 0.01876032, -0.032987};
                            //itemVector = VectorOperations.initializeVector(latentFeatures);
                        }
                        //******************************************************************************
                        //if(knownUser) {
                        //*******************************1-recommend top k items for the user*****************************************
                        //rate the coming user with all items
                        //output it on the side

                        Iterable<Map.Entry<String, Double[]>> itemsVectors = itemsMatrix.entries();
                        for (Map.Entry<String, Double[]> anItem : itemsVectors) {
                            Double score = Math.abs(1 - VectorsOp.VectorOperations.dot(userVector, anItem.getValue()));
                            itemsScoresMatrixMap.put(anItem.getKey(), score.floatValue());
                        }
                        //************************************************************************************************************
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        //send the maps to take the average then recommend and score
                        //context.output(scoreMapOutput, Tuple4.of(input.f0, user, item, itemsScoresMatrixMap));

                        //out.collect(Tuple3.of(user,item,itemsScoresMatrixMap));

                        // }


                        //*******************************Forgetting tecghnique************************************************************


                        if(status.value()){
                            //TODO: Remove this state
                            //if the first record only
                            generalCounter.update(context.timestamp());
                            status.update(false);
                        }

                        if(context.timestamp() >= generalCounter.value() + 22218811){

                            generalCounter.update(context.timestamp());
                            Map<String, Long> userFootPrintClone = new HashMap<>();
                            for(Map.Entry<String,Long> userMap : userFootPrint.entries()){
                                userFootPrintClone.put(userMap.getKey(),userMap.getValue());
                            }

                            for(Map.Entry<String,Long> userMap : userFootPrintClone.entrySet()){
                                //forget users
                                if(context.timestamp() >=  userMap.getValue()+22218811){
                                    usersMatrix.remove(user);
                                    ratedItemsByUser.remove(user);
                                    userFootPrint.remove(user);

                                    Map<String,Float> output = new HashMap<>();
                                    output.put("hhh",99999999f);

                                    out.collect(Tuple3.of("user","Heidy",output));
                                }
                            }


                            Map<String, Long> itemFootPrintClone = new HashMap<>();
                            for(Map.Entry<String,Long> itemMap : itemFootPrint.entries()){
                                itemFootPrintClone.put(itemMap.getKey(),itemMap.getValue());
                            }


                            for(Map.Entry<String,Long> itemMap : itemFootPrintClone.entrySet()){
                                if(context.timestamp() >=  itemMap.getValue()  + 22218811){

                                    Map<String,Float> output = new HashMap<>();
                                    output.put("hhh",99999999f);

                                    out.collect(Tuple3.of("item","Heidy",output));

                                    itemsMatrix.remove(item);
                                    itemFootPrint.remove(item);
                                }
                            }

                        }


                        userFootPrint.put(user,context.timestamp());
                        itemFootPrint.put(item,context.timestamp());


                        //*******************************3. update the model with the observed event**************************************

                        int k = 1;
                        if (k > 0) {
                            //TODO: Add the iteration loop
                            for (Integer l = 0; l < k; l++) {
                                userItemVectors = sgd.update_isgd2(userVector, itemVector, mu, lambda);
                                usersMatrix.put(user, userItemVectors.userVector);
                                itemsMatrix.put(item, userItemVectors.itemVector);
                            }
                        }
                    }


                    @Override
                    public void open(Configuration config) {


                        MapStateDescriptor<String, Double[]> descriptor2 =
                                new MapStateDescriptor<>(
                                        "itemMatrixDescriptor",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, Double[]> descriptor22 =
                                new MapStateDescriptor<>(
                                        "usersMatrix",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, ArrayList<String>> descriptor3 =
                                new MapStateDescriptor<>(
                                        "ratedItemsByUser",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<ArrayList<String>>() {
                                        })
                                );

                        ValueStateDescriptor<Integer> descriptor4 =
                                new ValueStateDescriptor<Integer>(
                                        "flag",
                                        Integer.class,
                                        0
                                );

                        MapStateDescriptor<String, Long> desriptor5 =
                                new MapStateDescriptor<String, Long>(
                                        "user(count)",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })

                                );
                        MapStateDescriptor<String, Long> desriptor6 =
                                new MapStateDescriptor<String, Long>(
                                        "item(count)",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })

                                );

                        ValueStateDescriptor<Long> descriptor7 =
                                new ValueStateDescriptor<Long>(
                                        "generalCounter",
                                        Long.class,
                                        0L
                                );

                        ValueStateDescriptor<Boolean> descriptor8 =
                                new ValueStateDescriptor<Boolean>(
                                        "status",
                                        Boolean.class,
                                        true
                                );

                        itemsMatrix = getRuntimeContext().getMapState(descriptor2);
                        usersMatrix = getRuntimeContext().getMapState(descriptor22);
                        ratedItemsByUser = getRuntimeContext().getMapState(descriptor3);
                        userFootPrint = getRuntimeContext().getMapState(desriptor5);
                        itemFootPrint = getRuntimeContext().getMapState(desriptor6);
                        generalCounter = getRuntimeContext().getState(descriptor7);
                        flagForInitialization = getRuntimeContext().getState(descriptor4);
                        generalCounter = getRuntimeContext().getState(descriptor7);
                        status = getRuntimeContext().getState(descriptor8);
                    }
                });






        return itemsScores;

    }

}

