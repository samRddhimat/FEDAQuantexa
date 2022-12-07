import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date,_}


object FEDAQuantexa {

  def main(args: Array[String]): Unit = {
    //for winutils
    System.setProperty("hadoop.home.dir","C:\\Users\\Srinivasan\\hadoop-common-2.2.0-bin-master\\bin") //<local path of winutils>")
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    /********FlightData <-- 1)Find the total number of flights for each month. ************/

    val dfFDRaw = spark.read.option("header",true).csv("src/resources/flightData.csv")
    //    dfFDRaw.printSchema()
    //    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val dfFDTyped = dfFDRaw.select(
      col("passengerId").alias("PID"),
      col( "flightId").cast(IntegerType).alias("FID"),
      col("from").alias("Origin"),
      col("to").alias("Destination"),
//      unix_timestamp(col("date"),"MM/dd/yyyy").cast(TimestampType).cast(DateType).alias("DateOfJourney")
//      , month(unix_timestamp(col("date"),"MM/dd/yyyy").cast(TimestampType)).as("TravelMonth")
//      to_date(col("date"),"yyyy-MM-dd").alias("JourneyDate"),
//      date_format(to_date(col("date"),"yyyy-MM-dd"),"MM").alias("JourneyMonth"))
    to_date(col("date"),"yyyy-MM-dd").alias("DateOfJourney"),
    date_format(to_date(col("date"),"yyyy-MM-dd"),"MM").alias("TravelMonth").cast(IntegerType)
    )
//    dfFDTyped.show(100)

//    dfFDTyped.printSchema()
    //    dfFDTyped.filter("flightId == 20").show(10)
    //    println("Hello World!")
    //    dfFDTyped.select(count("TravelMontH").as("NumberofFlights"), "TRavelMonth")
//    dfFDTyped.groupBy("TravelMonth","FID").count().as("Month").orderBy("TravelMonth").show(100)

    println("1) Find the total number of flights for each month - OUPUT STARTS")
        dfFDTyped.groupBy("TravelMonth").count().as("Month").orderBy("TravelMonth").show()
    println("1) Find the total number of flights for each month - OUPUT ENDS")

     /*End of Computation for question 1)*/

    /** 2)Find the names of the 100 most frequent flyers. */
    val dfPsngrRaw = spark.read.option("header",true).csv("src/resources/passengers.csv")
    dfPsngrRaw.printSchema()

    val dfPsngrTyped = dfPsngrRaw.select(
      col("passengerId").alias("PID"),
      col( "firstName").alias("First Name"),
      col("lastName").alias("Last Name")
    )
    println("Schema of dfPsngrTyped")
    dfPsngrTyped.printSchema()
//    dfPsngrTyped.show(500)

//    dfFDTyped.as("dfFDTyped").join(dfPsngrTyped.as("dfPsngrTyped"), $"dfFDTyped.PID" === $"dfPsngrTyped.PID", "left_semi")
    dfFDTyped.join(dfPsngrTyped,dfFDTyped("PID") === dfPsngrTyped("PID"))
        .select(dfFDTyped.columns.map(c => dfFDTyped(c)):_*).printSchema()

  }
}
