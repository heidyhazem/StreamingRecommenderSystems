package recommender;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;

import incrementalNeighbourhood.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sun.java2d.xr.XRUtils.None;


/**
 represents collaborative filtering recommender based on user's Neighbourhood with binary rating
 */
public class IncNeighbrCFRec extends recommenderAbstract {

    private DataStream<Tuple4<Integer, String, String, Float>> withKeyStream;

    /**
     * Constructs the input KeyedStream
     *
     * @param withKeyStream the input KeyedStream
     */
    public void IncNeighbrCFRec(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream) {
        withKeyStream = this.withKeyStream;
    }


    @Override
    public DataStream<Tuple2<String, Map<String, Float>>> fit(DataStream<Tuple4<Integer, String, String, Float>> withKeyStream) {


        DataStream<Tuple2<String, Map<String, Float>>> estimatedRateUserItemMap = withKeyStream.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple4<Integer, String, String, Float>, Tuple2<String, Map<String, Float>>>() {



                    /**
                     holds state for user's rating for items as we need to know which items the user rated to
                     calculate the user's similarities
                     */
                    MapState<String,Map<String,Float>> userItemRatingHistory;
                    /**
                     holds state for user's similarities parts to be incrementally updated((user,user),countOfCommonItems,countOfRatedItemsU1,countOfRatedItemsU2)
                     */
                    MapState<Tuple2<String,String>,Tuple4<Integer,Integer,Integer,Float>> userSimilarities;
                    /**
                     holds all items in the system used to get estimated rating for unrated items for the users
                     */
                    MapState<String,Integer> allItems;

                    @Override
                    public void processElement(Tuple4<Integer, String, String, Float> input, Context context, Collector<Tuple2<String, Map<String, Float>>> out) throws Exception {

                        String user =  input.f1;
                        String item = input.f2;
                        Float rate = input.f3;

                        //step1: Get top k similar users to the current users

                        //generate pairs
                        //prepare Map<user,sim>
                        //method to get top k users




                        //step2: Estimate rate from user to all unrated items




                        //TODO: Recommend state should be output before updating the state
                        //TODO: Refactor state (similarities can be eliminated)

                        //################################################################################################################################
                        //**********************//Step3: prepare similarities for recommendation before update **********************************

                        if(userItemRatingHistory.contains(user)){
                            /**
                             * Updating similarities state
                             */
                            //generate pairs
                            //step 2:1 similarities
                            for (String userInHistory : userItemRatingHistory.keys() ) {

                                //for not generating tuple with the same user
                                if( !user.equals(userInHistory)){

                                    //get the ordered pairOf users(key)
                                    Tuple3<String,String,Integer>  keyWithPosition = new GeneratePairs().getKey(user,userInHistory);
                                    Tuple2<String,String> userPair = Tuple2.of(keyWithPosition.f0,keyWithPosition.f1);
                                    Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2;

                                    //Step 2:2 : get Pair from state to update it incrementally
                                    Tuple4<Integer,Integer,Integer,Float> commonCount1Count2 = userSimilarities.get(userPair);



                                    //step 2:3 : check if the the other user like this item before or not
                                    //to update common Count (first number in the tuple) or not

                                    Integer commonCount = commonCount1Count2.f0;
                                    if(userItemRatingHistory.get(userInHistory).containsKey(item)){
                                        commonCount1Count2.setField(commonCount+1,0);
                                    }

                                    //step2:4 : increment the current user count of items
                                    Integer currentUserItemCount = commonCount1Count2.getField(positionOfCurrentUserInTheTuple+1);
                                    commonCount1Count2.setField(currentUserItemCount+1, positionOfCurrentUserInTheTuple+1);

                                    //step2:5 update sim state itself
                                    userSimilarities.put(userPair,commonCount1Count2);


                                    //userSimilarities
                                    Float cosSim =new IncrementalCosineSim().calculatecosineSimilarity(commonCount1Count2.f0,commonCount1Count2.f1,commonCount1Count2.f2);
                                    commonCount1Count2.setField(cosSim,3);
                                }

                            }

                            //******************************************************************************************************
                            //********************** Step3: Update other states  **************************************************
                            // userItemRatingHistory

                            Map<String,Float> toUpdateItems = userItemRatingHistory.get(user);
                            toUpdateItems.put(user,rate);
                            userItemRatingHistory.put(user,toUpdateItems);

                            //allItems
                            if(! allItems.contains(item)){
                                allItems.put(item,None);
                            }
                            //******************************************************************************************************


                        }
                        //user is not known
                        else{

                            Integer countCurrentUser = 1;
                            for (String userInHistory : userItemRatingHistory.keys() ) {

                                //Step 2:2 : Initialize similarity parts
                                Tuple4<Integer,Integer,Integer,Float> commonCount1Count2 = Tuple4.of(0,0,0,0f) ;

                                //get the ordered pairOf users(key)
                                Tuple3<String,String,Integer>  keyWithPosition = new GeneratePairs().getKey(user,userInHistory);
                                Tuple2<String,String> userPair = Tuple2.of(keyWithPosition.f0,keyWithPosition.f1);
                                Integer positionOfCurrentUserInTheTuple = keyWithPosition.f2 + 1;

                                //commonCount
                                if(userItemRatingHistory.get(userInHistory).containsKey(item)){
                                    commonCount1Count2.setField(1,0);
                                }

                                //CurrentUserCount new user so the count of items rated by user is one
                                commonCount1Count2.setField(1,positionOfCurrentUserInTheTuple);

                                //otherUserItemsCount
                                Integer otherUserItemsCount = userItemRatingHistory.get(userInHistory).size();
                                //get the position of the other item in pair
                                Integer positionOfTheOtherUser;
                                if(positionOfCurrentUserInTheTuple.equals(1)){
                                    positionOfTheOtherUser = 2;
                                }
                                else{
                                    positionOfTheOtherUser = 1;
                                }

                                commonCount1Count2.setField(otherUserItemsCount, positionOfTheOtherUser);

                                //userSimilarities
                                Float cosSim =new IncrementalCosineSim().calculatecosineSimilarity(commonCount1Count2.f0,commonCount1Count2.f1,commonCount1Count2.f2);
                                commonCount1Count2.setField(cosSim,3);
                            }


                            //********************** Step3: Update other states  **************************************************

                            // userItemRatingHistory
                            Map<String,Float> toUpdateItems = new HashMap<>();
                            toUpdateItems.put(item,rate);

                            userItemRatingHistory.put(user,toUpdateItems);

                            //allItems
                            if(! allItems.contains(item)){
                                allItems.put(item,None);
                            }

                        }

                        //################################################################################################################



                    }


                    @Override
                    public void open(Configuration config) {


                        MapStateDescriptor<Tuple2<String, String>, Tuple4<Integer, Integer,Integer,Float>> descriptor2 =
                                new MapStateDescriptor<>(
                                        "UserUserSimilarities",
                                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Tuple4<Integer, Integer,Integer,Float>>() {

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

                        MapStateDescriptor<String, Map<String, Float>> descriptor4 =
                                new MapStateDescriptor<>(
                                        "userItemRatingHistory",
                                        TypeInformation.of(new TypeHint<String>() {

                                        }),
                                        TypeInformation.of(new TypeHint<Map<String, Float>>() {

                                        })
                                );

                        userItemRatingHistory = getRuntimeContext().getMapState(descriptor4);
                        userSimilarities = getRuntimeContext().getMapState(descriptor2);
                        allItems = getRuntimeContext().getMapState(descriptor3);
                    }

                });



        return estimatedRateUserItemMap;
    }

    @Override
    public ArrayList<String> recommend(DataStream<Tuple2<String, Map<String, Float>>> estimatedRatesOfItems) {
        return null;
    }
}
