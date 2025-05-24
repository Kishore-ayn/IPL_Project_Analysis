/*
===============================================================================
Script: Top 3 batsmen per season 
===============================================================================
Script Purpose:
    This script filters out the top 3 batsmen per season by using some pyspark transformation
===============================================================================
*/

#Total runs per striker per season 
batsmen_runs_per_season = ball_by_ball_df.select("season", "strikersk", "runs_scored")\
                            .groupBy("season","strikersk").agg(sum("runs_scored").alias("total_runs"))

#Batsmen with name
batsmen_with_name = batsmen_runs_per_season.join(player_df, batsmen_runs_per_season["strikersk"] == player_df["player_sk"], "left").select("season", "player_name", "total_runs")

#Defining window specification
window_spec = Window.partitionBy("season").orderBy(col("total_runs").desc())

#Giving ranks order by scores descending 
batsmen_ranking = batsmen_with_name.withColumn("rank", row_number().over(window_spec))

#Filterting top 3 ranks season wise
top3_batsmen_per_season = batsmen_ranking.filter(col("rank") <= 3)

#Final output showing
top3_batsmen_per_season.show()
