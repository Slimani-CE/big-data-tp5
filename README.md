# TP5: Spark SQL

## Exercice 1 :
L'objectif de cet exercice est de traiter les données d'incidents de chaque service d'une entreprise industrielle. Les incidents sont stockés dans un fichier csv :
```
Id,titre,description,service,date
1,"Problème d'imprimante","L'imprimante du service comptabilité ne fonctionne pas",Comptabilité,2023-12-15
2,"Erreur de connexion","Les utilisateurs ne peuvent pas se connecter au réseau",IT,2023-12-17
3,"Panne de machine","La machine de production 3 est hors service",Production,2023-12-18
4,"Problème de logiciel","Le logiciel de gestion des stocks plante fréquemment",Logistique,2023-12-20
5,"Besoin de maintenance","Le système de climatisation doit être réparé",Maintenance,2023-12-21
6,"Problème d'imprimante","L'imprimante du service comptabilité ne fonctionne pas",Comptabilité,2023-12-15
7,"Erreur de connexion","Les utilisateurs ne peuvent pas se connecter au réseau",IT,2023-12-17
8,"Panne de machine","La machine de production 3 est hors service",Production,2023-12-18
```

### 1. Afficher le nombre d’incidents par service.
```java
public class Task1 {
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

        // Affichage du nombre d'incidents par service
        data.groupBy("service").count().show();

        // Fermeture de la session Spark
        spark.stop();
    }
}
```

#### Résultat ✅
![img.png](assets/img.png)

### 2. Afficher les deux années où il a y avait plus d’incidents.
```java
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
```

#### Résultat ✅
![img_1.png](assets/img_1.png)

## Exercice 2 : Traitement de données stockées dans Mysql
L'objectif de cet exercice est de traiter les données stockées dans une base de données relationnelle Mysql nommée DB_HOPITAL, qui contient trois tables PATIENTS, MEDECINS et CONSULTATIONS.

### Les tables
#### La table Consultation
![img_3.png](assets/img_3.png)

#### La table Medecins
![img_4.png](assets/img_4.png)

#### La table Patients
![img_5.png](assets/img_5.png)

### 1. Afficher le nombre de consultations par jour.
```java
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
```

#### Résultat ✅
![img_2.png](assets/img_2.png)

### 2. Afficher le nombre de consultation par médecin.
```java
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
        consultations.groupBy("ID").count().show();
    }
}
```

#### Résultat ✅
![img_6.png](assets/img_6.png)