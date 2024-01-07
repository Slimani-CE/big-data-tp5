package com.slimani.exercice2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;

public class Task3 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Task1")
                .master("local[*]")
                .getOrCreate();

        // L'écriture des données dans la table Medecins
        Dataset<Row> medecins = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "MEDECINS")
                .option("user", "root")
                .option("password", "")
                .load();

        // L'écriture des données dans la table Consultations
        Dataset<Row> consultations = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "CONSULTATIONS")
                .option("user", "root")
                .option("password", "")
                .load();

        // L'écriture des données dans la table Patients
        Dataset<Row> patients = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "PATIENTS")
                .option("user", "root")
                .option("password", "")
                .load();

        // Grouper les consultations par médecin et par patient pour compter le nombre de patients assistés par médecin
        Dataset<Row> countPerDoctor = consultations.groupBy("ID_MEDECIN", "ID_PATIENT")
                .agg(countDistinct("ID_PATIENT").alias("NUM_PATIENTS_ASSISTED"));

        // Jointure entre countPerDoctor et medecins pour afficher le nom et le prénom du médecin
        Dataset<Row> joinedDF = countPerDoctor.join(medecins, countPerDoctor.col("ID_MEDECIN")
                        .equalTo(medecins.col("ID")))
                .groupBy("NOM", "PRENOM")
                .agg(sum("NUM_PATIENTS_ASSISTED").alias("TOTAL_PATIENTS_ASSISTED"))
                .select("NOM", "PRENOM", "TOTAL_PATIENTS_ASSISTED");

        // Affichage du nombre de patients assistés par médecin
        joinedDF.show();

        // Fermeture de la session Spark
        spark.stop();
    }
}
