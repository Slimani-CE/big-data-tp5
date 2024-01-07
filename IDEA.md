TP: Spark SQL

Exercice 1 :
On souhaite développer pour une entreprise industrielle une application Spark qui traite les
incidents de chaque service. Les incidents sont stockés dans un fichier csv.
Le format de données dans les fichiers csv et la suivante :
Id, titre, description, service, date
Travail à faire :
1. Afficher le nombre d’incidents par service.
2. Afficher les deux années où il a y avait plus d’incidents.

Exercice 2 :
L’hôpital national souhaite traiter ces données au moyen d’une application Spark d’une
manière parallèle est distribuée. L’hôpital possède des données stockées dans une base de
données relationnelle et des fichiers csv. L’objectif est de traiter ces données en utilisant Spark
SQL à travers les APIs DataFrame et Dataset pour extraire des informations utiles afin de
prendre des décisions.
I. Traitement de données stockées dans Mysql
L’hôpital possède une application web pour gérer les consultations de ces patients, les
données sont stockées dans une base de données MYSQL nommée DB_HOPITAL, qui
contient trois tables PATIENTS, MEDECINS et CONSULTATIONS (Voir les figures 1, 2 et 3).

Big Data: Fondements et Architectures de stockage 2023


Figure 1: structure de la table PATIENTS
PATIENTS(ID, NOM, PRENOM, CIN, TEL, EMAIL, DATE_NASSAINCE)

Figure 2: structure de la table MEDECINS
MEDECINS(ID, NOM, PRENOM, TEL, EMAIL, SPECIALITE)

Figure 3: La table CONSULTATIONS
La table CONSULTATIONS(ID, ID_MEDICIN, ID_PATIENT, DATE_CONSULTATION)

Travail à faire :
Vous créez la base de données et les tables et vous répondez aux questions suivantes :
- Afficher le nombre de consultations par jour.
- Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant :
  NOM | PRENOM | NOMBRE DE CONSULTATION
- Afficher pour chaque médecin, le nombre de patients qu’il a assisté.