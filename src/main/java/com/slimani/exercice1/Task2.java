package com.slimani.exercice1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Task2 {
    public static void main(String[] args) {
        // L'application Spark qui affiche le nombre d’incidents par service.
        SparkSession spark = SparkSession
                .builder()
                .appName("Task1")
                .master("local[*]")
                .getOrCreate();

        // L'écriture du fichier CSV en tant que DataFrame
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/Incidents.csv");

        // Affichage de deux années où le nombre d'incidents est le plus élevé
        data.groupBy("date").count().orderBy(col("count").desc()).show(2);
    }
}
