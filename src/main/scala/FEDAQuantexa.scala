import org.antlr.v4.runtime.atn.SemanticContext
import org.antlr.v4.runtime.atn.SemanticContext.AND
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{collect_list, flatten, to_date}
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
    println("Converting raw flight data to typed dataset")
    val dfFDTyped = dfFDRaw.select(
      col("passengerId").cast(IntegerType).alias("PID"),
      col("flightId").cast(IntegerType).alias("FID"),
      col("from").alias("Origin"),
      col("to").alias("Destination"),
      date_format(to_date(col("date")),"dd-MM-yyyy").alias("DateOfJourney"),
      date_format( to_date(col("date")), "MM").alias("Month").cast(IntegerType)
    )//.sort(col("DateOfJourney").desc)

//    println("Showing below Flight Data (dfFDTyped) with proper types")
//    dfFDTyped.show(100)
//    println("Done showing Flight Data (dfFDTyped)")

/*** Enable below two line to see the schema of dfFDTyped **/
    //    println("Schema of dfFDTyped")
    //    dfFDTyped.printSchema()

    println("1) Find the total number of flights for each month - OUTPUT STARTS\n")
//    dfFDTyped.groupBy("TravelMonth").agg(count("*").alias("Number of Flights")).show()
    dfFDTyped.groupBy("Month")
      .agg(count("*").alias("Number of Flights"))
      .sort((col("Month").asc))
      .show()
    println("1) Find the total number of flights for each month - OUTPUT ENDS\n")

    /*End of Computation for question 1)*/

    /**** 2)Find the names of the 100 most frequent flyers. ****/

      //Loading raw data of Passenger.csv
    val dfPsngrRaw = spark.read.option("header", true).csv("src/resources/passengers.csv")
//    dfPsngrRaw.printSchema()
    /**"Converting raw passenger data to typed dataset"**/
//    println("Converting raw passenger data to typed dataset")
    val dfPsngrTyped = dfPsngrRaw.select(
      col("passengerId").cast(IntegerType).alias("PID"),
      col("firstName").alias("First Name"),
      col("lastName").alias("Last Name")
    )
    //    println("Schema of dfPsngrTyped")
    //    dfPsngrTyped.printSchema()

    /** Showing below Passenger Data (dfPsngrTyped) with proper types **/
//    println("Showing below Passenger Data (dfPsngrTyped) with proper types")
//    dfPsngrTyped.show(100)
//    println("Done showing Passenger Data (dfPsngrTyped)")
  print("2)Find the names of the 100 most frequent flyers. - OUTPUT STARTS")
    val dfTop100Traveller = dfFDTyped.groupBy("PID").agg(count("*")
      .alias("Number of Flights"))
      .sort(col("Number of Flights").desc)
      .limit(100)
//        println("Schema of dfTop100Traveller")
//        dfTop100Traveller.printSchema()
    /** Below are the Top 100 Travellers **/
    println("Below are the Top 100 Travellers")
    dfTop100Traveller
      .join( dfPsngrTyped, "PID")
      .select(dfTop100Traveller("PID").as("Passenger ID"), dfTop100Traveller("Number of Flights"), dfPsngrTyped("First Name"), dfPsngrTyped("Last Name"))
      .show(false)
    //    println("dfFDTyped merged value test")
println("2)Find the names of the 100 most frequent flyers - OUTPUT ENDS.")

    /**** 2)Find the names of the 100 most frequent flyers - ENDS. ****/

    /**** 3)greatest number of countries a passenger has been in without being in the UK - STARTS. ****/
//    println("Showing LongestRun")
//    val dfGreatest = dfFDTyped.groupBy("PID")
//      .agg(  collect_set(array("Origin","Destination"))
//      .alias("Longest Run"))
//      .withColumn ("Longest Run",concat_ws(",",
//     flatten(col("Longest Run"))))
//      .show(false)
    /** question 3 not completed **/

    /***** 4) Find the passengers who have been on more than 3 flights together. - OUTPUT STARTS***/
//  println("dfFDTyped.printSchema()")
//    dfFDTyped.printSchema()
//    println("dfFDTyped show")
    dfFDTyped.alias("df1").join(dfFDTyped.alias("df2"),
      col("df1.PID") < col("df2.PID") &&
        col("df1.FID") === col("df2.FID") &&
        col("df1.DateofJourney") === col("df2.DateofJourney"))
    .groupBy(col("df1.PID").alias("Passenger 1 ID"),
      col("df2.PID").alias("Passenger 2 ID"))
      .agg(count("*").as("Number of flights together")).
      where(col("Number of flights together") >= 3)
      .sort(col("Number of flights together").desc)
      .show()


  }
}