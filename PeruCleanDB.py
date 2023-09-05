import pandas as pd
import psycopg2
import logging
import os

logging.basicConfig(filename='PeruCleanData.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    
    db_config = {
    'dbname': os.getenv("DATABASE_NAME"),
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD"),
    'host': os.getenv("DATABASE_HOST")
    }

    # Establishing the connection
    conn = psycopg2.connect(**db_config)  

    cur = conn.cursor()

    # Create aggregated tables for different levels
    cur.execute('''
                CREATE TABLE IF NOT EXISTS distrito_death_positive_cases(
                    fecha_resultado DATE,
                    departamento TEXT,
                    provincia TEXT,
                    distrito TEXT,
                    num_death_cases INTEGER,
                    num_positive_cases INTEGER
                );
                CREATE TABLE IF NOT EXISTS province_death_positive_cases(
                    fecha_resultado DATE,
                    departamento TEXT,
                    provincia TEXT,
                    num_death_cases INTEGER,
                    num_positive_cases INTEGER 
                );
                CREATE TABLE IF NOT EXISTS department_death_positive_cases(
                    fecha_resultado DATE,
                    departamento TEXT,
                    num_death_cases INTEGER,
                    num_positive_cases INTEGER
                );  
                CREATE TABLE IF NOT EXISTS country_death_positive_cases(
                    fecha_resultado DATE,
                    num_death_cases INTEGER,
                    num_positive_cases INTEGER
                );
                ''')

    # Aggregate death and positive cases at different levels
    cur.execute('''
                WITH district_aggregated AS (
                    SELECT
                        COALESCE(d.fecha_resultado, p.fecha_resultado) AS fecha_resultado,
                        COALESCE(d.departamento, p.departamento) AS departamento,
                        COALESCE(d.provincia, p.provincia) AS provincia,
                        COALESCE(d.distrito, p.distrito) AS distrito,
                        COALESCE(d.num_death_cases, 0) AS num_death_cases,
                        COALESCE(p.num_positive_cases, 0) AS num_positive_cases
                    FROM distrito_death_cases d
                    FULL OUTER JOIN distrito_positive_cases p
                    ON d.fecha_resultado = p.fecha_resultado
                    AND d.departamento = p.departamento
                    AND d.provincia = p.provincia
                    AND d.distrito = p.distrito
                ),
                province_aggregated AS (
                    SELECT
                        COALESCE(d.fecha_resultado, p.fecha_resultado) AS fecha_resultado,
                        COALESCE(d.departamento, p.departamento) AS departamento,
                        COALESCE(d.provincia, p.provincia) AS provincia,
                        COALESCE(d.num_death_cases, 0) AS num_death_cases,
                        COALESCE(p.num_positive_cases, 0) AS num_positive_cases
                    FROM province_death_cases d
                    FULL OUTER JOIN province_positive_cases p
                    ON d.fecha_resultado = p.fecha_resultado
                    AND d.departamento = p.departamento
                    AND d.provincia = p.provincia
                ),
                department_aggregated AS (
                    SELECT
                        COALESCE(d.fecha_resultado, p.fecha_resultado) AS fecha_resultado,
                        COALESCE(d.departamento, p.departamento) AS departamento,
                        COALESCE(d.num_death_cases, 0) AS num_death_cases,
                        COALESCE(p.num_positive_cases, 0) AS num_positive_cases
                    FROM department_death_cases d
                    FULL OUTER JOIN department_positive_cases p
                    ON d.fecha_resultado = p.fecha_resultado
                    AND d.departamento = p.departamento
                ),
                country_aggregated AS (
                    SELECT
                        COALESCE(d.fecha_resultado, p.fecha_resultado) AS fecha_resultado,
                        COALESCE(d.num_death_cases, 0) AS num_death_cases,
                        COALESCE(p.num_positive_cases, 0) AS num_positive_cases
                    FROM country_death_cases d                 FULL OUTER JOIN country_positive_cases p
                    ON d.fecha_resultado = p.fecha_resultado
                )

                INSERT INTO distrito_death_positive_cases (fecha_resultado, departamento, provincia, distrito, num_death_cases, num_positive_cases)
                SELECT * FROM district_aggregated
                ORDER BY fecha_resultado ASC, departamento ASC, provincia ASC, distrito ASC;

                INSERT INTO province_death_positive_cases (fecha_resultado, departamento, provincia, num_death_cases, num_positive_cases)
                SELECT * FROM province_aggregated
                ORDER BY fecha_resultado ASC, departamento ASC, provincia ASC;

                INSERT INTO department_death_positive_cases (fecha_resultado, departamento, num_death_cases, num_positive_cases)
                SELECT * FROM department_aggregated
                ORDER BY fecha_resultado ASC, departamento ASC;

                INSERT INTO country_death_positive_cases (fecha_resultado, num_death_cases, num_positive_cases)
                SELECT * FROM country_aggregated
                ORDER BY fecha_resultado ASC;
                ''')

    conn.commit()

except (Exception, psycopg2.DatabaseError) as error:
    logging.error(f"Error: {error}")
    if conn is not None:
        conn.rollback()  # rollback changes in case of error
    raise  # re-raise the last exception

else:
    logging.info("SQL queries executed successfully")

finally:
    if conn is not None:
        conn.close()

    logging.info("Database connection closed.")
