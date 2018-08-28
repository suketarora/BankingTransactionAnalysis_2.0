import org.apache.spark._
import scala.math.BigDecimal
import scala.io.Source
import java.sql.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
// import spark.implicits._
import org.apache.spark.sql.functions._




object Main extends App {

 override def main(arg: Array[String]): Unit = {
   var sparkConf = new SparkConf().setMaster("local").setAppName("Transaction")
   var sc = new SparkContext(sparkConf)
       val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
  
   val resource = getClass.getResourceAsStream("/Transaction Sample data-1.csv")
    if (resource == null) sys.error("Please download the banking dataset 1")
     val firstRddRaw =  sc.parallelize(Source.fromInputStream(resource).getLines().toList)

    val resource2 = getClass.getResourceAsStream("/Transaction Sample data-2.csv")
    if (resource2 == null) sys.error("Please download the banking dataset 2")
     val secondRddRaw =  sc.parallelize(Source.fromInputStream(resource2).getLines().toList)
   
    import spark.implicits._

   // val firstRddRaw = sc.textFile("file:///home/suket/Documents/Transaction Sample data-1.csv")
   // val secondRddRaw = sc.textFile("file:///home/suket/Documents/Transaction Sample data-2.csv")
   val firstRdd = firstRddRaw.filter(x => !x.contains("Timestamp"))
   val secondRdd = secondRddRaw.filter(x => !x.contains("Timestamp"))
   val firstTransactionRdd = firstRdd.map(parse1)
   val secondTransactionRdd = secondRdd.map(parse2)
   val TransactionRdd = firstTransactionRdd.union(secondTransactionRdd)
   val TransactionRddDF = TransactionRdd.toDF.persist
   TransactionRddDF.createOrReplaceTempView("Transactions")
  

     // avgDeditTransactionAmount.head.schema.printTreeString

   val avgDeditTransactionAmount =spark.sql("SELECT avg(amount) FROM Transactions where nature = 'D'").collect.map { 
                                                                                                                     row => (BigDecimal(row(0).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
                                                                                                                     }
    // avgDeditTransactionAmount: Array[Double] = Array(25.423887122416534)

     

    val avgCreditTransactionAmount = TransactionRddDF.select("amount").where("nature=='C'").agg(avg("amount")).collect.map { 
                                                                                                                     row => (BigDecimal(row(0).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
                                                                                                                     }
   
    // avgCreditTransactionAmount: Array[Double] = Array(25.58844925043008)
    
    val yearwiseAvgCreditTransactions =spark.sql("SELECT year,avg(amount) FROM Transactions where nature = 'C' group by year order by year").collect.map { 
                                                                                                                     row => (row(0).asInstanceOf[Int],BigDecimal(row(1).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
                                                                                                                     }
   


  // yearwiseAvgCreditTransactions: Array[(Int, Double)] = Array((2012,25.8064730168092), (2013,25.577028124307965), (2014,25.382060534649092))
 
 val yearwiseAvgDebitTransactions = TransactionRddDF.groupBy($"year").avg("amount").orderBy("year").collect.map { 
                                                                                                                     row => (row(0).asInstanceOf[Int],BigDecimal(row(1).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
                                                                                                                     }
  // yearwiseAvgDebitTransactions: Array[(Int, Double)] = Array((2012,25.56683224718589), (2013,25.532384891595836), (2014,25.477403451109286))
 

 val BalanceSheet = TransactionRddDF.groupBy($"accountId").sum("amount").orderBy($"sum(amount)".desc).limit(1).collect.map { 
                                                                                                                     row => (row(0).asInstanceOf[Int],BigDecimal(row(1).asInstanceOf[Double]).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
                                                                                                                     }
  // BalanceSheet: Array[org.apache.spark.sql.Row] = Array([1207101,427.0])
  

  val highest = BalanceSheet(0)._1


                                                     /*val creditTransactions = TransactionRdd.filter( _.nature == "C")
                                                     val debitTransactions  = TransactionRdd.filter( _.nature == "D")
                                                     val avgCreditTransactionAmount = BigDecimal(creditTransactions.map(_.amount).reduce ( _ + _ ) / creditTransactions.count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                                                     val avgDeditTransactionAmount = BigDecimal(debitTransactions.map(_.amount).reduce ( _ + _ ) / debitTransactions.count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                                                     val yearAndCreditTransactions = creditTransactions.map { transaction => (transaction.timestamp.getYear,transaction)}
                                                     val yearwiseCreditTransactions =  yearAndCreditTransactions.groupByKey
                                                     val yearAndDebitTransactions = debitTransactions.map { transaction => (transaction.timestamp.getYear,transaction)}
                                                     val yearwiseDebitTransactions =  yearAndDebitTransactions.groupByKey
                                                     val yearwiseAvgCreditTransactions = yearwiseCreditTransactions.map {case (k,v) => (k,BigDecimal((v.map(_.amount).reduce( _ + _))/v.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}.collect.toList
                                                     val yearwiseAvgDebitTransactions = yearwiseDebitTransactions.map {case (k,v) => (k,BigDecimal((v.map(_.amount).reduce( _ + _))/v.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}.collect.toList
                                                     val account = TransactionRdd.map { transaction => (transaction.accountId,transaction)}
                                                     val accountWiseTransactions = account.groupByKey
                                                          // (accountId,CB[Transactions])
                                                      val BalanceSheet = accountWiseTransactions.map { case (k,v) => (k,v.map(transactionProcessor).reduce ( _ + _))}.collect.toList
                                                      val highest = BalanceSheet.maxBy(_._2)._1*/

                                                     println()
                                                     println("Average amount of all Credit Transactions = " +avgCreditTransactionAmount(0))
                                                     println("Average amount of all Debit Transactions = " +avgDeditTransactionAmount(0))
                                                     println()
                                                    for ( count <- 0 to yearwiseAvgCreditTransactions.length-1){

                                                     var tuple = yearwiseAvgCreditTransactions(count)
                                                     var year = tuple._1
                                                     var avgCredit = tuple._2
                                                     println("Year = "+year +"    Average amount of all Credit Transactions = "+avgCredit)
                                                     }

                                                    println()
                                                    for ( count <- 0 to yearwiseAvgDebitTransactions.length-1){

                                                     var tuple = yearwiseAvgDebitTransactions(count)
                                                     var year = tuple._1
                                                     var avgDebit = tuple._2
                                                     println("Year = "+year +"    Average amount of all Debit Transactions = "+avgDebit)
                                                    

                                                    }



                                                     println("accountId whose balance amount is highest = "+highest)

                                                     println()
                                                    
                                                     




   sc.stop()
   
 }

 case class Transaction(accountId: Int, name: String,timestamp: Date,year:Int, nature: String, amount: Double) extends Serializable with Ordered[Transaction]{
   def compare(that:Transaction) = timestamp.toString().compare(that.timestamp.toString())

 }

  // def transactionProcessor(transaction: Transaction):Double = {
  //   if(transaction.nature == "C")  return transaction.amount
  //   transaction.amount * -1

  // }

  def parse1(row: String): Transaction = {
   val fields = row.split(",")
   val accountId: Int = fields(0).toInt
   val name: String = fields(1)
   val Array(d,m,y) = fields(2).split("/")
   val timestamp: Date = java.sql.Date.valueOf( y+"-"+m+"-"+d)
   val year: Int = y.toInt
   val nature: String = fields(3) 
   var amount: Double = fields(4).substring(1).toDouble
   if (nature == "D"){amount = amount * -1}
   Transaction(accountId,name,timestamp,year,nature,amount)
   
 }
 
 def parse2(row: String): Transaction = {
   val fields = row.split(",")
   val accountId: Int = fields(0).toInt
   val name: String = fields(1)
   val Array(d,m,y) = fields(3).split("/")
   val timestamp: Date = java.sql.Date.valueOf( y+"-"+m+"-"+d)
   val nature: String = fields(4) 
   val year: Int = y.toInt
   var amount: Double = fields(5).substring(1).toDouble
   if (nature == "D"){amount = amount * -1}
   Transaction(accountId,name,timestamp,year,nature,amount)
   
 }

}



