package aueb.bigdata.project


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, col, round, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ImdbProject {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("movies")
      .master("local[*]")
      .getOrCreate()

    val options = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|");

    // 1
    val movies = options.csv("data/MOVIES_GENRE.txt")
    val users = options.csv("data/USERS.txt")
    val ratings = options.csv("data/USER_MOVIES.txt")
    val superDS = movies.join(ratings, "mid").join(users, "userid")

    // use cube instead?
    val genreGenderView = superDS.groupBy("genre", "gender").agg(avg("rating").alias("rating"))
    writeDFtoFile("genre_gender_view", genreGenderView)

    // refactor to roll up
    val genreView = genreGenderView.groupBy("genre").agg(avg("rating").alias("rating"))
    writeDFtoFile("genre_view", genreView)

    // refactor to roll up
    val genderView = genreGenderView.groupBy("gender").agg(avg("rating").alias("rating"))
    writeDFtoFile("gender_view", genderView)

    // refactor to roll up
    val avgRatingView = genreView.select(avg("rating"))
    writeDFtoFile("avg_rating", avgRatingView)

    // 2
    val maleGenreRatings = genreGenderView.select("genre", "rating").where("gender = 'M'").withColumnRenamed("rating", "male_rating")
    val femaleGenreRatings = genreGenderView.select("genre", "rating").where("gender = 'F'").withColumnRenamed("rating", "female_rating")
    maleGenreRatings.join(femaleGenreRatings, "genre").filter("female_rating > male_rating").select("genre").show(false)

    // 3 Find better way, declare rdd schema
    val crimeRatings = superDS.filter("genre='Crime'").select(round(col("rating")).alias("rounded_rating")).groupBy("rounded_rating").count()
    val crimeRatingsSum = crimeRatings.select(sum(col("count"))).head().getLong(0);
    val normalizedCrimeRatings = crimeRatings.withColumn("percentage", round(col("count").divide(crimeRatingsSum).multiply(100)))
      .select("rounded_rating", "percentage").sort("rounded_rating")
    val slices = normalizedCrimeRatings.rdd.map(row => (row.getAs[Int](0).toString.concat(" star"), row.getAs[Double](1).toInt)).collectAsMap()
    println(Chart.drawPieChart(slices))
  }

  def writeDFtoFile(fileName: String, dataFrame: DataFrame): Unit = {
    dataFrame.coalesce(1)
      .write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"target/views/${fileName}")
  }
}
