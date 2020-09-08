package Forgetting;

import Matrix.Row;
import Matrix.SparseBinaryMatrix;
import incrementalNeighbourhood.GeneratePairs;
import incrementalNeighbourhood.IncrementalCosineSim;
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
import recommender.RecommenderUtilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LFU{

    public DataStream<Tuple3<Integer,String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream, Integer k){

        DataStream<Tuple3<Integer,String, Map<String, Float>>> estimatedRateUserItemMap = withKeyStream.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple3<Integer, String, Map<String, Float>>>() {

                    /**
                     * holds state for user's rating for items as we need to know which items the user rated to
                     * calculate the user's similarities
                     */
                    ValueState<SparseBinaryMatrix> userItemRatingHistory;

                    /**
                     * holds state for user's similarities parts to be incrementally updated((user,user),countOfCommonItems,countOfRatedItemsU1,countOfRatedItemsU2)
                     */
                    MapState<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Float>> userSimilarities;

                    /**
                     * holds whether timeStamp or count
                     */
                    MapState<String, Long> userFootPrint;
                    MapState<String, Long> itemFootPrint;
                    ValueState<Long> generalCounter;

                    ValueState<Boolean> firstTimeFlag;
                    ValueState<Integer> rowNumber;
                    ValueState<Integer> colNumber;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple3<Integer, String, Map<String, Float>>> out) throws Exception {

                        String user = input.f1;
                        String item = input.f2;
                        Float rate = input.f3;


                        if(firstTimeFlag.value()){
                            // lw de awl mara item we user yegy ll system y
                            //ybaa lazm a initiate sparse matrix

                            //user gdid + item gdid + en da awl pair yegy el system

                            SparseBinaryMatrix userItemMatrix = new SparseBinaryMatrix(1, 1);
                            userItemMatrix.appendRow(new Row(new int[]{0}));
                            userItemMatrix.assignRowLabel(0,user);
                            userItemMatrix.assignColumnLabel(0,item);
                            userItemMatrix.setValue(user,item);

                            userItemMatrix.colNumber = 1;
                            userItemMatrix.rowNumber = 1;

                            /*//update them
                            Integer colNo = colNumber.value()+1;
                            Integer rowNo = rowNumber.value()+1;
                            colNumber.update(colNo+1);
                            rowNumber.update(rowNo+1);*/

                            //Update the state
                            userItemRatingHistory.update(userItemMatrix);
                            firstTimeFlag.update(false);
                        }
                        else{

                            if(userItemRatingHistory.value().containsUser(user)){
                                //prepare Map<user,sim>
                                Map<String, Float> currentUserSimilaritiesMap = new HashMap<>();

                                //###################### get top k similar user#######################################################

                                //generate pairs
                                for (String userInHistory : userItemRatingHistory.value().getRowLabels()) {
                                    //get the ordered pairOf users(key)
                                    Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(user, userInHistory);
                                    Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                                    //Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                                    try {
                                        Float sim = userSimilarities.get(userPair).f3;
                                        currentUserSimilaritiesMap.put(userInHistory, sim);
                                    }
                                    catch (Exception e) {
                                        System.out.println("The pair is not exist");
                                    }
                                }

                                //method to get top k users
                                ArrayList<Tuple2<String, Float>> topKSimilarUsers = new RecommenderUtilities().getMostKuSimilar(currentUserSimilaritiesMap, k);

                                //step2: Estimate rate from user to all unrated items

                                Map<String, Float> estimatedRatesForItems = new HashMap<>();

                                estimatedRatesForItems = new RecommenderUtilities().estimateRateForItems(
                                        userItemRatingHistory.value().getColumnLabels(), topKSimilarUsers, userItemRatingHistory.value(), user);

                                //htl3 el map de ashan ytrtbo we ytl3 recommendation
                                out.collect(Tuple3.of(input.f0, item, estimatedRatesForItems));
                            }


                            //_________________________________________________________________________________________________________________________________
                            //_________________________________________________________________________________________________________________________________
                            //Forgetting
                            //_________________________________________________________________________________________________________________________________
                            //_________________________________________________________________________________________________________________________________

                            //update similarities
                            SparseBinaryMatrix userItemsMatrix = userItemRatingHistory.value();
                            if (userItemsMatrix.containsUser(user)) {
                                /**
                                 * Updating similarities state
                                 */
                                //generate pairs
                                //step 2:1 similarities
                                for (String userInHistory : userItemsMatrix.getRowLabels()) {
                                    //for not generating tuple with the same user
                                    if (!user.equals(userInHistory)) {
                                        //get the ordered pairOf users(key)
                                        Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(user, userInHistory);
                                        Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                                        Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                                        //Step 2:2 : get Pair from state to update it incrementally
                                        Tuple4<Integer, Integer, Integer, Float> commonCount1Count2 = userSimilarities.get(userPair);


                                        //step 2:3 : check if the the other user like this item before or not
                                        //to update common Count (first number in the tuple) or not

                                        Integer commonCount = commonCount1Count2.f0;
                                        if(userItemsMatrix.ratedPositively(userInHistory,item)){
                                           commonCount1Count2.setField(commonCount + 1, 0);
                                        }

                                        //step2:4 : increment the current user count of items
                                        Integer currentUserItemCount = commonCount1Count2.getField(positionOfCurrentUserInTheTuple + 1);
                                        commonCount1Count2.setField(currentUserItemCount + 1, positionOfCurrentUserInTheTuple + 1);


                                        //userSimilarities
                                        Float cosSim = new IncrementalCosineSim().calculatecosineSimilarity(commonCount1Count2.f0, commonCount1Count2.f1, commonCount1Count2.f2);

                                        commonCount1Count2.setField(cosSim, 3);

                                        //step2:5 update sim state itself
                                        userSimilarities.put(userPair, commonCount1Count2);
                                    }
                                }

                                //******************************************************************************************************
                                //********************** Step3: Update other states  **************************************************

                                userItemsMatrix.setValueWithCheck(user,item);
                                userItemRatingHistory.update(userItemsMatrix);


                                //******************************************************************************************************

                            }
                            else{


                                Integer countCurrentUser = 1;
                                for (String userInHistory : userItemsMatrix.getRowLabels()) {

                                    //Step 2:2 : Initialize similarity parts
                                    Tuple4<Integer, Integer, Integer, Float> commonCount1Count2 = Tuple4.of(0, 0, 0, 0f);

                                    //get the ordered pairOf users(key)
                                    Tuple3<String, String, Integer> keyWithPosition = new GeneratePairs().getKey(user, userInHistory);
                                    Tuple2<String, String> userPair = Tuple2.of(keyWithPosition.f0, keyWithPosition.f1);
                                    Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2 + 1;

                                    //commonCount
                                    if (userItemsMatrix.ratedPositively(userInHistory,item)) {
                                        commonCount1Count2.setField(1, 0);
                                    }

                                    //CurrentUserCount new user so the count of items rated by user is one
                                    commonCount1Count2.setField(1, positionOfCurrentUserInTheTuple);

                                    //otherUserItemsCount
                                    Integer otherUserItemsCount = userItemsMatrix.getPositiveColumns(userInHistory).size();

                                    //get the position of the other item in pair
                                    Integer positionOfTheOtherUser;
                                    if (positionOfCurrentUserInTheTuple.equals(1)) {
                                        positionOfTheOtherUser = 2;
                                    } else {
                                        positionOfTheOtherUser = 1;
                                    }

                                    commonCount1Count2.setField(otherUserItemsCount, positionOfTheOtherUser);

                                    //userSimilarities
                                    Float cosSim = new IncrementalCosineSim().calculatecosineSimilarity(commonCount1Count2.f0, commonCount1Count2.f1, commonCount1Count2.f2);

                                    Map<String, Float> y = new HashMap<>();
                                    y.put("item", Float.valueOf(cosSim));
                                    //out.collect(Tuple3.of(999,userInHistory,y));

                                    commonCount1Count2.setField(cosSim, 3);
                                    userSimilarities.put(userPair, commonCount1Count2);

                                }


                                //update the states

                                userItemsMatrix.setValueWithCheck(user,item);
                                userItemRatingHistory.update(userItemsMatrix);
                            }
                        }
                    }

                    @Override
                    public void open(Configuration config) throws Exception {


                        MapStateDescriptor<Tuple2<String, String>, Tuple4<Integer, Integer, Integer, Float>> descriptor2 =
                                new MapStateDescriptor<>(
                                        "UserUserSimilarities",
                                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Float>>() {

                                        })
                                );

                        MapStateDescriptor<String, Integer> descriptor3 =
                                new MapStateDescriptor<>(
                                        "allItems",
                                        TypeInformation.of(new TypeHint<String>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Integer>() {

                                        })
                                );



                        ValueStateDescriptor<SparseBinaryMatrix> descriptor4 =
//                                new ValueStateDescriptor<SparseBinaryMatrix>(
//                                        "UserItemRatingHistory",
//                                        TypeInformation.of(new TypeHint<SparseBinaryMatrix>() {
//                                        }
//                                ));
                        new ValueStateDescriptor<SparseBinaryMatrix>("UserItemRatingHistory",SparseBinaryMatrix.class);

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
                                        "first Time Flag",
                                        Boolean.class,
                                        true
                                );

                        ValueStateDescriptor<Integer> descriptor9 =
                                new ValueStateDescriptor<Integer>(
                                        "row Number",
                                        Integer.class,
                                        0
                                );

                        ValueStateDescriptor<Integer> descriptor10 =
                                new ValueStateDescriptor<Integer>(
                                        "column Number",
                                        Integer.class,
                                        0
                                );


                        userItemRatingHistory = getRuntimeContext().getState(descriptor4);
                        userSimilarities = getRuntimeContext().getMapState(descriptor2);
                        userFootPrint = getRuntimeContext().getMapState(desriptor5);
                        itemFootPrint = getRuntimeContext().getMapState(desriptor6);
                        generalCounter = getRuntimeContext().getState(descriptor7);
                        firstTimeFlag = getRuntimeContext().getState(descriptor8);
                        rowNumber = getRuntimeContext().getState(descriptor9);
                        colNumber = getRuntimeContext().getState(descriptor10);

                    }


                });


        return estimatedRateUserItemMap;
    }

}