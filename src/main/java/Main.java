import dto.Audit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import scala.collection.immutable.Seq;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Main {


    static String[] stauss = {"Budgetary","Formal","Draft","Pending Approval","Approved","Pending Customer Accept","Customer Accepted","Order Submitted","Rejected","Cancelled","Expired"};

    static List<String > status= Arrays.asList( stauss );
    public static void main(String[] arg){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Quote Aging")
                .getOrCreate();

        JavaRDD<Audit> auditRDD = spark.read()
                            .json("src/main/resources/audit.json")
                            .javaRDD()
                            .map(line -> {
                                return getStatus( line);}).filter(audit -> {return (audit == null) ? false : true; })
                            .sortBy(audit -> {return audit.getDbLastUpdatedDate();},true,1 );


        Dataset<Row> auditDF = spark.createDataFrame(auditRDD, Audit.class);
        WindowSpec ws = Window.orderBy("dbLastUpdatedDate");
        UserDefinedFunction mode = udf(
                (String priviousDate,String currentDate) -> {

                    if(priviousDate== null || currentDate ==null){
                        return "0.0";
                    }else{
                        Date dt1 = sdf.parse(priviousDate);
                        Date dt2 = sdf.parse(currentDate);
                        double d = (dt2.getTime() - dt1.getTime())*1.0 /(24*60*60*1000);

                        return String.format("%.5f", d);
                    }

                    }, DataTypes.StringType
        );


        Dataset<Row> auditDF2 = auditDF
                                .withColumn("priviousDbLastUpdatedDate",lag("dbLastUpdatedDate",1).over(ws))
                               .select(col("currentStatus"),col("priviousStatus"),col("dbLastUpdatedDate"), mode.apply(col("priviousDbLastUpdatedDate"),col("dbLastUpdatedDate")).as("dayCount") ) ;

        auditDF2.show();
     /*   auditDF.createOrReplaceTempView("audit");
        Dataset<Row> teenagersDF = spark.sql("SELECT * FROM audit where auditLog = '2*C19*ATTRIB_392*N16*Formal2*O19*Budgetary' ");
        teenagersDF.show()*/;
       // spark.newSession().sql("SELECT * FROM global_temp.people").show();

    }

    private static Audit getStatus(Row line){

        String log = line.getString(0);
        String[] s = log.split("2\\*");
        if(s!= null && s.length>1){

            String[] dess= s[s.length-2].split("\\*");
            String[] srcss = s[s.length-1].split("\\*");

            if(dess.length>1 && status.contains(dess[1]) && srcss.length>1 && status.contains(srcss[1])){
                Audit audit= new Audit();
                audit.setAuditLog(line.getString(0));
                audit.setDbLastUpdatedDate(line.getString(1));
                audit.setCurrentStatus(dess[1]);
                audit.setPriviousStatus(srcss[1]);
                if(srcss[1].equalsIgnoreCase(stauss[0]) && dess[1].equalsIgnoreCase(stauss[1]) ){
                    audit.setDaysCount(0.0);
                }
                return audit;
            }

        }

        return null;
    }

}
