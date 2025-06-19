package com.scb.fdp.ingestion.loaders.jdbc

import com.scb.fdp.ingestion.constants.IngestionConstants
import com.scb.fdp.ingestion.db.DbConfig
import com.scb.fdp.ingestion.loaders.AbstractDataLoader
import org.apache.spark.sql.DataFrame
import com.scb.fdp.ingestion.utils.DateUtil
import com.scb.fdp.ingestion.loaders.LauncherMain
import java.net.{Socket, InetSocketAddress}
import scala.util.{Try,Success,Failure}

import scala.io.Source
import java.util.Properties
import org.apache.hadoop.fs.{FileSystem, Path}

class JdbcLoader extends AbstractDataLoader {

  override def readSource: DataFrame = {
    val numMaxRetries = 30
    println("Table info " + tableInfo)
    lazy val envFromLauncherMain = LauncherMain.env
    lazy val propertiesFileLocation = tableInfo.getConfigVal("config_location").replace("$env", envFromLauncherMain)
    lazy val dbConfig = new DbConfig(propertiesFileLocation)

    val sqlPath = tableInfo.getConfigVal(IngestionConstants.HdfsorNasPath).replace("$env", envFromLauncherMain) // + "/" + tableInfo.getConfigVal(IngestionConstants.SourceTableFileName)
    val sqlQueryTemp =  Source.fromFile(sqlPath).mkString("(", "", ") jdbc_query")
    var sqlQuery = sqlQueryTemp.replace("var_ods_date","'" + configContext.getOdsDate + "'")

    val s4dateInYYYYMMddDate = DateUtil.getDateInRequiredFormat(configContext.getOdsDate,
      DateUtil.YYYY_MM_DD_FORMAT, DateUtil.YYYYMMDD_FORMAT)
    sqlQuery = sqlQuery.replace("s4dateInYYYYMMdd","'" + s4dateInYYYYMMddDate + "'")
    println(s"Sql path: ${sqlPath} and sql query: ${sqlQuery}")

    val source = tableInfo.getConfigVal(IngestionConstants.Source)
    println(s"Source $source and TPSYS $getTpSys")

    val props = {
      val p = new Properties()
      val source = Source.fromFile(propertiesFileLocation)
      p.load(source.bufferedReader())
      p
    }

    //var jdbcDF = spark.read.format("jdbc").options(Map("url" -> dbConfig.getDbUrl, "user" -> dbConfig.getDbUser , "password" -> dbConfig.getPassword, "dbtable" ->  sqlQuery , "driver" -> dbConfig.getDbDriver)).load
    var jdbcDF = if (source.contains("RAH") && ( getTpSys.contains("IFRS") || getTpSys.contains("RAH"))) {
      println("Inside RAH JDBC Loader")
      val rah_hosts = props.getProperty("host").split(",").map(_.trim).toList
      println(s"Rah hosts: ${rah_hosts.mkString(", ")}")
      def findWorkingHost(): Option[String] = {

        rah_hosts.find { host =>

          Try {

            val jdbcUrl = s"jdbc:teradata://$host/DATABASE=SCP01_AES_VR"

            spark.read.format("jdbc").options(Map("url" -> jdbcUrl,
              "user" -> dbConfig.getDbUser,
              "password" -> dbConfig.getPassword,
              "driver" -> dbConfig.getDbDriver,
              "dbtable" -> "(SELECT 1 AS test_result) AS connectivity_test",
              "ENCRYPTDATA" -> "ON",
              "CHARSET" -> "UTF8",
              "LOGMECH" -> "LDAP"
            )).load().count()
            true
          }.recover {
            case e: Exception =>
              println(s"JDBC check failed for $host : ${e.getMessage}")
              false
          }.get
        }
      }
      val activeHost = findWorkingHost().getOrElse(throw new Exception("All hosts failed jdbc check"))
      println(s"Using host: $activeHost")
      spark.read.format("jdbc").options(Map("url" -> s"jdbc:teradata://$activeHost/DATABASE=SCP01_AES_VR", "user" -> dbConfig.getDbUser , "password" -> dbConfig.getPassword, "dbtable" ->  sqlQuery , "driver" -> dbConfig.getDbDriver,"ENCRYPTDATA" -> "ON","CHARSET" -> "UTF8","LOGMECH" -> "LDAP")).load
    } else {
      spark.read.format("jdbc").options(Map("url" -> dbConfig.getDbUrl, "user" -> dbConfig.getDbUser , "password" -> dbConfig.getPassword, "dbtable" ->  sqlQuery , "driver" -> dbConfig.getDbDriver)).load
    }

    if ( getTpSys.equals("IFRS_EBBS") || getTpSys.equals("IFRS_CCMS") || getTpSys.equals("IFRS_RLS") ) {
      var recCount = jdbcDF.count()
      var numTries = 0
      while (recCount == 0 && numTries <= numMaxRetries) {
        println(s"Sleeping for 15 mins as current count is zero  ${recCount}. Current Retry count: ${numTries}")
        // TO
        Thread.sleep(900000)
        println("Repulling the data now ")
        //jdbcDF = spark.read.format("jdbc").options(Map("url" -> dbConfig.getDbUrl, "user" -> dbConfig.getDbUser , "password" -> dbConfig.getPassword, "dbtable" ->  sqlQuery , "driver" -> dbConfig.getDbDriver)).load
        recCount = jdbcDF.count()
        numTries = numTries + 1
      }
      if ( numMaxRetries < numTries && recCount == 0 ) {
        println("=====================FAILING THIS JOB due to zero count So we need to retrigger it again ======")
        System.exit(1)
      }
    }
    jdbcDF.printSchema()
    jdbcDF.show()
    Globals.df = jdbcDF
    buildHistoryViewIfRequired(jdbcDF)
  }
}

object JdbcLoader {
  def apply(): JdbcLoader = new JdbcLoader()
}
