# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def main():
    """
    Cette methode est le point d'entrée de mon job.
    Elle va essentiellement faire 3 choses:
        - recuperer les arguments passés via la ligne de commande
        - creer une session spark
        - lancer le traitement
    Le traitement ne doit pas se faire dans la main pour des soucis de testabilité.
    """
    parser = argparse.ArgumentParser(
        description='Discover driving sessions into log files.')
    parser.add_argument('-e', "--etablissements", help='chemin vers le fichier qui contient etablissement input file', required=True)
    parser.add_argument('-f', "--valeurs_foncieres", help='chemin vers les valeurs foncières input file', required=True)
    parser.add_argument('-o', '--output', help='Output file', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    process(spark,args.etablissements,args.valeurs_foncieres,args.output)

def process(spark, etablissements, valeurs_foncieres, output):
        """
            Contient les traitements qui permettent de lire,transformer et sauvegarder mon resultat.
            :param spark: la session spark
            :param etablissements :  le chemin du dataset des etablissements.csv
            :param valeurs_foncières: le chemin du dataset des valeurs_foncières.csv
            :param output: l'emplacement souhaité du resultat
            """
            # Opérations sur le jeu de données des établissements
            # chargement du jeu de données des établissements
        df_ET = spark.read.option('header','true').option('delimiter',';').option('inferSchema','true').csv(etablissements)
            #chargement et visualisation du jeu de données foncières
        df_AP = spark.read.option('header','true').option('delimiter',',').option('inferSchema','true').csv(valeurs_foncieres)
            # Jointure entre les deux tableaux en utilisant la colonne Commune du dataset établissements et la colonne nom_commune
        df = df_ET.join(df_AP,df_ET.Commune == df_AP.nom_commune,'full')
            #L'instruction précédente fait la jointure des lignes où les communes sont identiques et rajoute le reste de lignes où il n'y a pas l'égalité
            #On améliore la performance du requêtage en utilisant la technologie parquet
        df.write.parquet(output)

if __name__ == '__main__':
     main()

