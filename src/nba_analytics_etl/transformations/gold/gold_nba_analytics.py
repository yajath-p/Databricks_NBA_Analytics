from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, avg, round, when, broadcast, sum

@dp.table (
    name="nba_gold_historical_roster"
)
def load_rosters():
    df_players = spark.read.table("nba_silver_players").select("PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "SEASON")

    df_teams = spark.read.table("nba_silver_teams").select("TEAM_ID","ABBREVIATION","NICKNAME","CITY")

    return df_players.join(df_teams, on="TEAM_ID", how="inner")


@dp.table (
    name="nba_gold_player_game_performance"
)
def load_player_stats():
    df_games = spark.read.table("nba_silver_games").select("GAME_DATE_EST", "GAME_ID", "SEASON", "HOME_TEAM_ID", "VISITOR_TEAM_ID")
    df_games_details = spark.read.table("nba_silver_games_details")
    
    return df_games.join(df_games_details, on="GAME_ID", how="inner")

@dp.table(name="nba_gold_player_opponent_stats")
def player_opponent_stats():
    df_perf = dp.read("nba_gold_player_game_performance")
    df_teams = dp.read("nba_silver_teams").select("TEAM_ID", "CITY", "NICKNAME")
   
    df_with_opp_id = df_perf.withColumn(
        "OPP_TEAM_ID",
        when(col("TEAM_ID") == col("HOME_TEAM_ID"), col("VISITOR_TEAM_ID"))
        .otherwise(col("HOME_TEAM_ID"))
    )

    df_with_opp_names = df_with_opp_id.join(
        broadcast(df_teams).withColumnRenamed("CITY", "OPP_CITY")
                           .withColumnRenamed("NICKNAME", "OPP_NAME")
                           .withColumnRenamed("TEAM_ID", "OPP_TEAM_ID"),
        on="OPP_TEAM_ID",
        how="left"
    )
    
    return df_with_opp_names

@dp.table(name="nba_gold_player_season_stats")
def player_season_stats():
    return (
        dp.read("nba_gold_player_game_performance")
        .groupBy("PLAYER_ID", "PLAYER_NAME", "SEASON")
        .agg(
            round(avg("PTS"), 2).alias("avg_pts"),
            round(avg("AST"), 2).alias("avg_ast"),
            round(avg("REB"), 2).alias("avg_reb"),
            count("GAME_ID").alias("games_played")
        )
    )

@dp.table(name="nba_gold_team_season_stats")
def load_team_season_stats():
    df_games = spark.read.table("nba_silver_games")
    df_teams = spark.read.table("nba_silver_teams").select("TEAM_ID", "CITY", "NICKNAME", "ABBREVIATION")

    home_stats = df_games.select(
        col("HOME_TEAM_ID").alias("TEAM_ID"),
        col("SEASON"),
        col("PTS_home").alias("PTS"),
        col("HOME_TEAM_WINS").alias("WIN_VAL"),
        col("FG_PCT_home").alias("FG_PCT")
    )


    away_stats = df_games.select(
        col("VISITOR_TEAM_ID").alias("TEAM_ID"),
        col("SEASON"),
        col("PTS_away").alias("PTS"),
        when(col("HOME_TEAM_WINS") == 0, 1).otherwise(0).alias("WIN_VAL"),
        col("FG_PCT_away").alias("FG_PCT")
    )

    all_stats = home_stats.union(away_stats)

    team_season_summary = (
        all_stats.groupBy("TEAM_ID", "SEASON")
        .agg(
            count("*").alias("games_played"),
            sum("WIN_VAL").alias("total_wins"),
            round(avg("PTS"), 2).alias("avg_pts_per_game"),
            round(avg("FG_PCT"), 4).alias("avg_fg_pct")
        )
        .withColumn("total_losses", col("games_played") - col("total_wins"))
        .withColumn("win_pct", round(col("total_wins") / col("games_played"), 3))
    )

    return team_season_summary.join(df_teams, on="TEAM_ID", how="inner")
