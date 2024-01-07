package com.slimani.exercice2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

// L'application Spark qui affiche le nombre de consultations par medecin
// Sachant qu'on a une table Medecins[NOM,PRENOM,ID,EMAIL,TEL,SPECIALITE]
// Une table Consultations[ID,ID_MEDECIN,ID_PATIENT,DATE_CONSULTATION]
// Une table Patients[ID,PRENOM,CIN,NOM,TEL,EMAIL,DATE_NAISSANCE]
public class Task2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Task1")
                .master("local[*]")
                .getOrCreate();

        // L'écriture des données dans un SGBD relationnel utilisant une jointure entre Medecins et Consultations
        Dataset<Row> consultations = spark
                .read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("query", "SELECT MEDECINS.ID, NOM, PRENOM FROM CONSULTATIONS INNER JOIN MEDECINS ON CONSULTATIONS.ID_MEDECIN = MEDECINS.ID")
                .option("user", "root")
                .option("password", "")
                .load();

        // Affichage du nombre de consultations par medicine
        consultations.groupBy(col("ID").as("ID_MEDECIN"), col("NOM"), col("PRENOM")).count().show();
    }
}
