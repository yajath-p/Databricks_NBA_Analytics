from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, when, coalesce




@dp.table (
    name="nba_silver_games"
)
def load_games():
    return (
        spark.read.table("nba_bronze_games")
        .select(col("GAME_DATE_EST").cast("date"),
                col("GAME_ID").cast("long"),
                col("GAME_STATUS_TEXT").cast("string"),
                col("HOME_TEAM_ID").cast("long"),
                col("VISITOR_TEAM_ID").cast("long"), 
                col("SEASON").cast("int"),
                col("PTS_home").cast("int"),
                col("FG_PCT_home").cast("double"),
                col("FT_PCT_home").cast("double"),
                col("FG3_PCT_home").cast("double"),
                col("AST_home").cast("int"),
                col("REB_home").cast("int"),
                col("PTS_away").cast("int"),
                col("FG_PCT_away").cast("double"),
                col("FT_PCT_away").cast("double"),
                col("FG3_PCT_away").cast("double"),
                col("AST_away").cast("int"),
                col("REB_away").cast("int"),
                col("HOME_TEAM_WINS").cast("int")
                )
        .withColumn("ingest_timestamp", current_timestamp())
    )

@dp.table(
    name="nba_silver_games_details"
)
def load_games_details():
    return (
        spark.read.table("nba_bronze_games_details")
        .select(
            col("GAME_ID").cast("long"),
            col("TEAM_ID").cast("long"),
            col("TEAM_ABBREVIATION").cast("string"),
            col("TEAM_CITY").cast("string"),
            col("PLAYER_ID").cast("long"),
            col("PLAYER_NAME").cast("string"),
            col("START_POSITION").cast("string"),
            col("COMMENT").cast("string"),
            col("MIN").cast("string"),
            col("FGM").cast("int"),
            col("FGA").cast("int"),
            col("FG_PCT").cast("double"),
            col("FG3M").cast("int"),
            col("FG3A").cast("int"),
            col("FG3_PCT").cast("double"),
            col("FTM").cast("int"),
            col("FTA").cast("int"),
            col("FT_PCT").cast("double"),
            col("OREB").cast("int"),
            col("DREB").cast("int"),
            col("REB").cast("int"),
            col("AST").cast("int"),
            col("STL").cast("int"),
            col("BLK").cast("int"),
            col("TO").cast("int"),
            col("PF").cast("int"),
            col("PTS").cast("int"),
            col("PLUS_MINUS").cast("int")
        )
        .withColumn("ingest_timestamp", current_timestamp())
    )

@dp.table(
    name="nba_silver_players"
)
def load_players():
    return (
        spark.read.table("nba_bronze_players")
        .select(col("PLAYER_NAME").cast("string"),
                col("TEAM_ID").cast("long"),
                col("PLAYER_ID").cast("long"),
                col("SEASON").cast("int"))
        .withColumn("ingest_timestamp", current_timestamp())
    )


@dp.table(
    name="nba_silver_ranking"
)
def load_ranking():
    return (
        spark.read.table("nba_bronze_ranking")
        .select(col("TEAM_ID").cast("long"),
                col("LEAGUE_ID").cast("long"),
                col("SEASON_ID").cast("long"),
                col("STANDINGSDATE").cast("date"),
                col("CONFERENCE").cast("string"),
                col("TEAM").cast("string"),
                col("G").cast("int"),
                col("W").cast("int"),
                col("L").cast("int"),
                col("W_PCT").cast("double"),
                col("HOME_RECORD").cast("string"),
                col("ROAD_RECORD").cast("string"),
                col("RETURNTOPLAY").cast("string"))
        .withColumn("ingest_timestamp", current_timestamp())
    )

@dp.table(
    name="nba_silver_teams"
)
def load_teams():
    #Cleans Null Vals
    arena_fix = (
        when(col("CITY") == "New Orleans", 17791)
        .when(col("CITY") == "Brooklyn", 17732)
        .when(col("CITY") == "Phoenix", 20106)
        .when(col("CITY") == "Philadelphia", 21000)
    )
    #Orlando was 0 so I manually added the capacity
    arena_fix_2 = (
        when(col("CITY") == "Orlando", 18846)
        .otherwise(col("ARENACAPACITY"))
    )

    return (
        spark.read.table("nba_bronze_teams")
        .withColumn("ARENACAPACITY", coalesce(col("ARENACAPACITY").cast("int"), arena_fix))
        .withColumn("ARENACAPACITY", arena_fix_2)
        .select(col("LEAGUE_ID").cast("long"),
                col("TEAM_ID").cast("long"),
                col("MIN_YEAR").cast("int"),
                col("MAX_YEAR").cast("int"),
                col("ABBREVIATION").cast("string"),
                col("NICKNAME").cast("string"),
                col("YEARFOUNDED").cast("int"),
                col("CITY").cast("string"),
                col("ARENA").cast("string"),
                "ARENACAPACITY",
                col("OWNER").cast("string"),
                col("GENERALMANAGER").cast("string"),
                col("HEADCOACH").cast("string"),
                col("DLEAGUEAFFILIATION").cast("string"))
        .withColumn("ingest_timestamp", current_timestamp())
    )