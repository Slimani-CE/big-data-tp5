package com.slimani.exercice2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// L'application Spark qui affiche le nombre de consultations par jour
public class Task1 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Task1")
                .master("local[*]")
                .getOrCreate();

        // L'écriture des données dans un SGBD relationnel
        Dataset<Row> consultations = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "CONSULTATIONS")
                .option("user", "root")
                .option("password", "")
                .load();

        // Affichage du nombre de consultations par jour
        consultations.groupBy("DATE_CONSULTATION").count().show();
    }
}
