package Recommender;

import Forgetting.LFU_DISGD;
import Forgetting.LRU_DISGD;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
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

public class DISGD extends RecommenderAbstract{



    @Override
    public DataStream<Tuple3<Integer, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k) {


        DataStream<Tuple3<Integer, String, Map<String, Float>>> itemsScores = withKeyStream.
                keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<Integer, String, Map<String, Float>>>() {


                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;

                    //int latentFeatures = 10;
                    Double lambda = 0.01;
                    Double mu = 0.05;

                    //number of recommended items
                    //Integer N = 10;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<Integer, String, Map<String, Float>>> out) throws Exception {

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

                        out.collect(Tuple3.of(input.f0,item,itemsScoresMatrixMap));

                        // }
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

                        itemsMatrix = getRuntimeContext().getMapState(descriptor2);
                        usersMatrix = getRuntimeContext().getMapState(descriptor22);
                        ratedItemsByUser = getRuntimeContext().getMapState(descriptor3);
                        flagForInitialization = getRuntimeContext().getState(descriptor4);
                    }
                });



        return itemsScores;
    }


    public DataStream<Tuple3<String, String, Map<String, Float>>> fit2(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k) {


        DataStream<Tuple3<String, String, Map<String, Float>>> itemsScores = withKeyStream.
                keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<String, String, Map<String, Float>>>() {


                    MapState<String, Double[]> itemsMatrix;
                    MapState<String, Double[]> usersMatrix;
                    MapState<String, ArrayList<String>> ratedItemsByUser;
                    ValueState<Integer> flagForInitialization;

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

                        out.collect(Tuple3.of(user,item,itemsScoresMatrixMap));

                        // }
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

                        MapStateDescriptor<String, Double[]> descriptor1 =
                                new MapStateDescriptor<>(
                                        "itemMatrixDescriptor",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Double[]>() {
                                        })
                                );

                        MapStateDescriptor<String, Double[]> descriptor2 =
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

                        itemsMatrix = getRuntimeContext().getMapState(descriptor1);
                        usersMatrix = getRuntimeContext().getMapState(descriptor2);
                        ratedItemsByUser = getRuntimeContext().getMapState(descriptor3);
                        flagForInitialization = getRuntimeContext().getState(descriptor4);
                    }
                });



        return itemsScores;
    }


    @Override
    public DataStream<Tuple3<Integer, String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingTechnique) {
        return null;
    }

    public DataStream<Tuple3<String, String, Map<String, Float>>> fit2(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k, String forgettingWay) {

        if(forgettingWay.equals("LRU")){
            return new LRU_DISGD().fit2(withKeyStream,k,"LRU");
        }

        else if(forgettingWay.equals("LFU")){
            return new LFU_DISGD().fit2(withKeyStream,k,"LFU");
        }

        else {
            return null;
        }
    }




    @Override
    public DataStream<Tuple3<Integer, String, ArrayList<String>>> recommend(DataStream<Tuple3<Integer, String, Map<String, Float>>> estimatedRatesOfItems, Integer k) {
        return null;
    }

    public DataStream<Tuple3<String, String, ArrayList<String>>> recommend2(DataStream<Tuple3<String, String, Map<String, Float>>> estimatedRatesOfItems, Integer k) {

        DataStream<Tuple3<String, String, ArrayList<String>>> recommendedItems = estimatedRatesOfItems.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<String, String, Map<String, Float>>, Tuple3<String, String, ArrayList<String>>>() {

                    MapState<String, Void> ratedItems;

                    @Override
                    public void processElement(Tuple3<String, String, Map<String, Float>> input, Context context, Collector<Tuple3<String, String, ArrayList<String>>> out) throws Exception {
                        Map<String, Float> AllitemsWithScore = new HashMap<>();
                        // Integer counterCheckNode = 0;
                        String currentUser = input.f0;
                        String currentItem = input.f1;

                        //************************************Avg Scores************************************************************************

                        for (Map.Entry<String, Float> userItemScore : input.f2.entrySet()) {
                            //TODO:Get the top n from each node
                            //test first with all items
                            if (ratedItems.contains(userItemScore.getKey())) {
                                continue;
                            } else {
                                AllitemsWithScore.put(userItemScore.getKey(), userItemScore.getValue());
                            }

                        }

                        //here we are ready for scoring and recommendation
                        //*******************************2-Score the recommendation list given the true observed item i***************
                        ArrayList<String> recommendedItems = new ArrayList<>();
                        Integer N = 10;

                        AllitemsWithScore.entrySet().stream().sorted(Map.Entry.comparingByValue())
                                .limit(N)
                                .forEach(itemRate -> recommendedItems.add(itemRate.getKey()));

                        out.collect(Tuple3.of(currentUser,currentItem,recommendedItems));

                        ratedItems.put(currentItem, null);

                        //************************************************************************************************************
                    }

                    @Override
                    public void open(Configuration config) {

                        MapStateDescriptor<String, Void> descriptor =
                                new MapStateDescriptor<>(
                                        "ratedItems",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Void>() {
                                        })
                                );

                        ratedItems = getRuntimeContext().getMapState(descriptor);
                    }
                });


        return recommendedItems;
    }
}