import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date,collect_list,flatten}
//import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

object FEDAQuantexa {

  def main(args: Array[String]): Unit = {
    //for winutils
    System.setProperty("hadoop.home.dir","C:\\Users\\Srinivasan\\hadoop-common-2.2.0-bin-master\\bin") //<local path of winutils>")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /********FlightData <-- 1)Find the total number of flights for each month. ************/

    val dfFDRaw = spark.read.option("header",true).csv("src/resources/flightData.csv")
    //Loading raw data of flightData.csv
    //    dfFDRaw.printSchema()
    //    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    println("Converting raw flight data to typed dataset")
    val dfFDTyped = dfFDRaw.select(
      col("passengerId").cast(IntegerType).alias("PID"),
      col("flightId").cast(IntegerType).alias("FID"),
      col("from").alias("Origin"),
      col("to").alias("Destination"),
//      to_date(col("date")).alias("DateOfJourney"),
//      date_format( to_date(col("date"), "yyyy-MM-dd"), "MM").alias("TravelMonth").cast(IntegerType)
      date_format(to_date(col("date")),"dd-MM-yyyy").alias("DateOfJourney"),
      date_format( to_date(col("date")), "MM").alias("TravelMonth").cast(IntegerType)
    )//.sort(col("DateOfJourney").desc)

    println("Showing below Flight Data (dfFDTyped) with proper types")
    dfFDTyped.show(100)
    println("Done showing Flight Data (dfFDTyped)")

    //    println("Schema of dfFDTyped")
    //    dfFDTyped.printSchema()
//        dfFDTyped.filter("flightId == 20").show(10)
    //    println("Hello World!")
    //    dfFDTyped.select(count("TravelMontH").as("NumberofFlights"), "TRavelMonth")
    //    dfFDTyped.groupBy("TravelMonth","FID").count().as("Month").orderBy("TravelMonth").show(100)

    println("1) Find the total number of flights for each month - OUTPUT STARTS")
    //    dfFDTyped.groupBy("TravelMonth").count().as("Month").orderBy("TravelMonth").show(100)
    dfFDTyped.groupBy("TravelMonth").agg(count("*")).alias("Number of Flights").show()
    println("1) Find the total number of flights for each month - OUTPUT ENDS")

    /*End of Computation for question 1)*/

    /**** 2)Find the names of the 100 most frequent flyers. ****/
      //Loading raw data of Passenger.csv
    val dfPsngrRaw = spark.read.option("header", true).csv("src/resources/passengers.csv")
//    dfPsngrRaw.printSchema()

    println("Converting raw passenger data to typed dataset")
    val dfPsngrTyped = dfPsngrRaw.select(
      col("passengerId").cast(IntegerType).alias("PID"),
      col("firstName").alias("First Name"),
      col("lastName").alias("Last Name")
    )
    //    println("Schema of dfPsngrTyped")
    //    dfPsngrTyped.printSchema()
    println("Showing below Passenger Data (dfPsngrTyped) with proper types")
    dfPsngrTyped.show(100)
    println("Done showing Passenger Data (dfPsngrTyped)")


    val dfTop100Traveller = dfFDTyped.groupBy("PID").agg(count("*")
      .alias("Number of Flights"))
      .sort(col("Number of Flights").desc)
      .limit(100)
    //    println("Schema of dfTop100Traveller")
    //    dfTop100Traveller.printSchema()
    println("Below are the Top 100 Travellers")
    //    dfTop100Traveller.show(100)
    //    println("Schema of dfTop100Traveller after merge")
    dfTop100Traveller
      .join( dfPsngrTyped, "PID").select(dfTop100Traveller("*"), dfPsngrTyped("First Name"), dfPsngrTyped("Last Name"))
      .show(false)
    //    println("dfFDTyped merged value test")

    /**** 2)Find the names of the 100 most frequent flyers - ENDS. ****/

    /**** 2)greatest number of countries a passenger has been in without being in the UK - STARTS. ****/
    //      dfFDTyped.groupBy("PID").agg(concat_ws("->", collect_list("Origin"))).alias("merged").show(false)
    val dfGreatest = dfFDTyped.groupBy("PID")
      .agg(collect_set(array("Origin","Destination"))
      .alias("Itinerary"))
      .withColumn ("Itinerary",concat_ws(",",
     flatten(col("Itinerary"))))

//    dfGreatest.withColumn("LongestRun",
//    size(array_distinct(col("Itinerary"). ))).show()

//      .show(false)
//    dfFDTyped.groupBy("PID")
//      .agg(collect_set(array("Origin","Destination")))
//      .alias("merged").show(false)
    /***** 4) Find the passengers who have been on more than 3 flights together. ***/


//    dfPsngrTyped.as("df1").join(dfPsngrTyped.as("df2"),
//      $"df1.PID" < $"df2.PID" &&
//        $"df1.FID" === $"df2.FID" &&
//        $"df1.DateofJourney" === $"df2.DateofJourney",
//      "inner"
//    ).
//      groupBy($"df1.PID", $"df2.PID")
//      .agg(count("*").as("flightsTogether"),
//        min($"df1.DateofJourney").as("from"),
//        max($"df1.DateofJourney").as("to")).
//      where($"flightsTogether" >= 3).
//      show()

  }
}