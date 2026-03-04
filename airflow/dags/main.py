from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import logging
import requests
import os
from dotenv import load_dotenv


load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))


# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# Liste des villes à analyser
VILLES = ['Dakar', 'Libreville', 'Paris', 'New York', 'Tokyo', 'London']

# URL de l'API OpenWeather
URL_API = "https://api.openweathermap.org/data/2.5/weather"


with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description="Pipeline ETL pour collecter et analyser les données météo",
    start_date=datetime(2026, 3, 4),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'weather', 'openweather']
) as dag:
    
    @task
    def extract():
        try:
            API_KEY = os.getenv('OPENWEATHER_API_KEY')
            
            if not API_KEY:
                logging.error("ERREUR: La clé API n'est pas définie dans le fichier .env")
                raise ValueError("Clé API manquante")
            
            logging.info("=" * 50)
            logging.info("DÉBUT DE L'EXTRACTION")
            logging.info("=" * 50)
            
            donnees_meteo = []
            
            for ville in VILLES:
                parametres = {
                    'q': ville,
                    'appid': API_KEY,
                    'units': 'metric',
                    'lang': 'fr'
                }
                
                reponse = requests.get(URL_API, params=parametres)
                reponse.raise_for_status()
                data = reponse.json()
                
                enregistrement = {
                    'ville': data['name'],
                    'pays': data['sys']['country'],
                    'temperature': data['main']['temp'],
                    'ressenti': data['main']['feels_like'],
                    'humidite': data['main']['humidity'],
                    'pression': data['main']['pressure'],
                    'vitesse_vent': data['wind']['speed'],
                    'description': data['weather'][0]['description'],
                    'visibilite': data.get('visibility', 0),
                    'lever_soleil': data['sys']['sunrise'],
                    'coucher_soleil': data['sys']['sunset']
                }
                
                donnees_meteo.append(enregistrement)
                logging.info(f"✓ Données extraites pour {ville}")
            
            
            df = pd.DataFrame(donnees_meteo)
            file_path = "/tmp/raw_weather_data.csv"
            df.to_csv(file_path, index=False)
            
            logging.info(f"✓ {len(df)} enregistrements extraits et sauvegardés dans {file_path}")
            return file_path
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Erreur lors de l'appel API: {e}")
            raise
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction: {e}")
            raise
    
    @task
    def transform(file_path):
        try:
            logging.info("=" * 50)
            logging.info("DÉBUT DE LA TRANSFORMATION")
            logging.info("=" * 50)
            
            
            df = pd.read_csv(file_path)
            
            # Transformation 1: Convertir les timestamps en dates lisibles
            df['lever_soleil_dt'] = pd.to_datetime(df['lever_soleil'], unit='s')
            df['coucher_soleil_dt'] = pd.to_datetime(df['coucher_soleil'], unit='s')
            logging.info("1. Conversion des timestamps")
            
            # Transformation 2: Calculer la durée du jour
            df['duree_jour_heures'] = ((df['coucher_soleil'] - df['lever_soleil']) / 3600).round(2)
            logging.info("2. Calcul de la durée du jour")
            
            # Transformation 3: Catégorie de température
            def categorie_temp(temp):
                if temp < 0:
                    return 'Très froid'
                elif temp < 10:
                    return 'Froid'
                elif temp < 20:
                    return 'Frais'
                elif temp < 30:
                    return 'Agréable'
                else:
                    return 'Chaud'
            
            df['categorie_temp'] = df['temperature'].apply(categorie_temp)
            logging.info("3. Catégorisation des températures")
            
            # Transformation 4: Indice de confort
            df['indice_confort'] = (df['temperature'] - (df['humidite'] / 10)).round(2)
            logging.info("4. Calcul de l'indice de confort")
            
            # Transformation 5: Convertir visibilité en km
            df['visibilite_km'] = (df['visibilite'] / 1000).round(2)
            logging.info("5. Conversion visibilité en km")
            
            # Transformation 6: Ajouter la date d'extraction
            df['date_extraction'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info("6. Ajout de la date d'extraction")
            
            # Sauvegarder les données transformées
            transformed_file_path = '/tmp/transformed_weather_data.csv'
            df.to_csv(transformed_file_path, index=False)
            
            logging.info(f"Données transformées et sauvegardées dans {transformed_file_path}")
            return transformed_file_path
            
        except Exception as e:
            logging.error(f"Erreur lors de la transformation: {e}")
            raise
    
    @task
    def load(transformed_file_path):
        try:
            logging.info("=" * 50)
            logging.info("DÉBUT DU CHARGEMENT")
            logging.info("=" * 50)
            
            # Charger les données transformées
            df = pd.read_csv(transformed_file_path)
            
            # Chemin de la base de données dans AIRFLOW_HOME
            airflow_home = os.getenv('AIRFLOW_HOME', os.path.dirname(os.path.dirname(__file__)))
            db_path = os.path.join(airflow_home, 'weather_data.db')
            
            logging.info(f"Base de données: {db_path}")
            
            
            conn = sqlite3.connect(db_path)
            
            df.to_sql('meteo', conn, if_exists='replace', index=False)
            
            # Vérifier le chargement
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM meteo")
            nombre = cursor.fetchone()[0]
            
            logging.info(f"{nombre} enregistrements chargés dans '{db_path}'")
            
            # Afficher un aperçu
            apercu = pd.read_sql_query(
                "SELECT ville, temperature, categorie_temp, humidite, indice_confort FROM meteo", 
                conn
            )
            logging.info("\n=== Aperçu des données ===")
            logging.info(f"\n{apercu.to_string()}")
            
            conn.close()
            
            logging.info("=" * 50)
            logging.info("PIPELINE ETL TERMINÉ AVEC SUCCÈS !")
            logging.info("=" * 50)
            
            return True
            
        except Exception as e:
            logging.error(f"Erreur lors du chargement: {e}")
            raise
    

    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task)

    extract_task >> transform_task >> load_task
