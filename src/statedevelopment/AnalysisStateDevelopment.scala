/*
 * Application to perform analysis on state development
 */

package statedevelopment

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

object AnalysisStateDevelopment {
  def main(args: Array[String]): Unit = {

    //specify the configuration for the spark application using instance of SparkConf
    val config = new SparkConf().setAppName("Assignment 20.2").setMaster("local")

    //setting the configuration and creating an instance of SparkContext
    val sc = new SparkContext(config)

    //Entry point of our sqlContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //to use toDF method
    import sqlContext.implicits._

    /*
     * create the rdd from textfile and get the columns and convert it to Dataframe using toDF method and register the same to temp table census
     */
    val census_data = sc.textFile("/home/acadgild/sridhar_scala/assignment/census.csv").map(x => x.split(",")).map(x => (x(0), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22))).toDF("State", "Persons", "Males", "Females", "Growth_1991_2001", "Rural", "Urban", "Scheduled_Caste_population", "Percentage_SC_to_total", "Number_of_households", "Household_size_per_household", "Sex_ratio_females_per_1000_males ", "Sex_ratio_0_6_years", "Scheduled_Tribe_population", "Percentage_to_total_population_ST", "Persons_literate", "Males_Literate", "Females_Literate", "Persons_literacy_rate", "Males_Literatacy_Rate", "Females_Literacy_Rate", "Total_Educated").registerTempTable("census")

    //1. Find out the state wise population and order by state

    /*
     * get the sum of persons and group by state to get the total population by state
     */
    val population = sqlContext.sql("select state,sum(persons) as total_population from census group by state order by total_population desc").show

    //2. Find out the Growth Rate of Each State Between 1991-2001

    /*
     * get the growth rate by using aggregate function avg on Growth_1991_2001 column grouping by state
     */
    val growth_rate = sqlContext.sql("select state,avg(Growth_1991_2001) as total_growth from census group by state").show

    //3. Find the literacy rate of each state

    /*
     * get the literacy rate of each state by using aggregate function avg on Persons_literacy_rate grouping by state
     */
    val literacy = sqlContext.sql("select state,avg(Persons_literacy_rate) from census group by state").show

    //4. Find out the States with More Female Population

    /*
     * get the States with More Female Population by using aggregate function sum and grouping by state
     */
    val female_pop = sqlContext.sql("select state, sum(Males)-sum(Females) from census group by state").show

    //5. Find out the Percentage of Population in Every State
    
    /*
     * get the Percentage of Population in Every State by using aggregate sunction sum and window function sum() over
     */

    val percenet_pop = sqlContext.sql("select state, (sum(persons) * 100.0) / SUM(sum(persons)) over() as percent_pop_by_state from census group by state").show

  }
}