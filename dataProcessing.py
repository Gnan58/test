import database as db
import logging
import numpy as np
import requests
import pandas as pd
import io
import os
from datetime import date, time
from datetime import timedelta

# Configure the logging module to write logs to a file
logging.basicConfig(filename='peruRawData.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def collect_csv(temp_url):
    try:
        # Response collects the file from the specified url. Have to add User Agent to the url in order to allow access.
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        response = requests.get(temp_url, headers=headers)

        # Collects data into a dataframe and delimits data before assigning feature names.
        temp_df = pd.read_csv(io.StringIO(response.content.decode('utf-8')), parse_dates=True)
        return temp_df
    except Exception as e:
        logging.error(f"Error in collect_csv: {e}")
        return None

def clean_dataframe(temp_df, file_name, temp_file_index, temp_features, today_str):
    try:
        # Death, hospitalizations, and vaccinations data does not need to be delimited and requires different cleaning.
        if file_name != 'DHV_' + today_str + '.csv':
            # Delimits data when a semi-colon appears.
            temp_df.columns = ['Data']
            temp_df = temp_df['Data'].str.split(';', expand=True)

            # Inserts temp_features as column headers and then drops NA values. Also assigns fecha_resulatado as a datetime.
            temp_df.columns = [temp_features[temp_file_index]]
            temp_df = temp_df.replace(r'^\s*$', pd.NA, regex=True)
            temp_df = temp_df.dropna()
            temp_df['fecha_resultado'] = temp_df['fecha_resultado'].astype('datetime64[ns]')
        else:
            # Inserts date that the data was collected and assigns columns headers.
            temp_df.columns = [temp_features[temp_file_index]]
            temp_df.insert(1, 'fecha_recopilacion', today_str)

            # Drops last four columns as they are redundant.
            temp_df = temp_df.iloc[:, :-4]

        temp_df.drop_duplicates(keep=False, inplace=True)

        # Cleans Classification type for deaths since special characters appear in current data.
        if file_name == 'Deaths_' + today_str + '.csv' or file_name == 'DHV_' + today_str + '.csv':
            # Replacing values with special characters with more readable variations.
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio virolÃ³gico', 'Virological', temp_df.criterio_fallecido)
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio SINADEF', 'SINADEF', temp_df.criterio_fallecido)
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio serolÃ³gico', 'Serological', temp_df.criterio_fallecido)
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio investigaciÃ³n EpidemiolÃ³gica', 'Epidemiological investigation', temp_df.criterio_fallecido)
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio clÃ\xadnico', 'Clinical', temp_df.criterio_fallecido)
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio radiolÃ³gico', 'Radiological', temp_df.criterio_fallecido)
            temp_df['criterio_fallecido'] = np.where(temp_df['criterio_fallecido'] == 'Criterio nexo epidemiolÃ³gico', 'Epidemiological link', temp_df.criterio_fallecido)

        for column in temp_df.columns:
            if column != 'fecha_resultado':
                temp_df[column] = temp_df[column].astype(str)
        return temp_df
    except Exception as e:
        logging.error(f"Error in clean_dataframe: {e}")
        return None

def main():
    conn = db.create_connection()
    if conn is None:
        logging.error("Unable to establish a connection with the database")
        return

    urls = ['https://files.minsa.gob.pe/s/eRqxR35ZCxrzNgr/download', 'https://files.minsa.gob.pe/s/t9AFqRbXw3F55Ho/download', 'https://cloud.minsa.gob.pe/s/8EsmTzyiqmaySxk/download']
    file_names = ['Positive_Cases_', 'Deaths_', 'DHV_']
    features = [['fecha_recopilacion', 'departmento', 'provincia', 'distrito', 'metodo', 'edad', 'sexo', 'fecha_resultado', 'UBIGEO', 'UUID'],
        ['fecha_recopilacion', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'departamento', 'provincia', 'distrito', 'UBIGEO', 'UUID'],
        ['UUID', 'fecha_resultado', 'edad', 'sexo', 'criterio_fallecido', 'UBIGEO', 'departamento', 'provincia', 'distrito',
        'cdc_positividad', 'flag_vacuna', 'fecha_dosis1', 'fabricante_dosis1', 'fecha_dosis2',
        'fabricante_dosis2', 'fecha_dosis3', 'fabricante_dosis3', 'flag_hospitalizado', 'eess_renaes', 'eess_diresa',
        'eess_red', 'eess_nombre', 'fecha_ingreso_hosp', 'flag_uci', 'fecha_ingreso_uci', 'fecha_ingreso_ucin',
        'con_oxigeno', 'con_ventilacion', 'fecha_segumiento_hosp_ultimo', 'evolucion_hosp_ultimo', 'ubigeo_inei_domicilio',
        'dep_domicilio', 'prov_domicilio', 'dist_domicilio']]

    # Index for iterating through file name list/features and gets current time.
    file_index = 0
    table_names = ['positive_cases', 'deaths', 'dhv']
    today = date.today()
    today_str = str(today)

    for url in urls:
        file_names[file_index] += today_str
        file_names[file_index] += ".csv"

        try:
            # Get CSV from specified url and places it into a dataframe.
            df = collect_csv(url)

            # Puts data through a definition to clean the data.
            df = clean_dataframe(df, file_names[file_index], file_index, features, today_str)

            if df is not None:
                df.to_csv(file_names[file_index], index=False)
                # Update the table with new data from the DataFrame.
                db.create_table_if_not_exists(conn, table_names[file_index], df)
                # Insert the data from the DataFrame into the table.
                db.insert_data(conn, table_names[file_index], df)

            else:
                logging.error(f"Unable to process DataFrame for {table_names[file_index]}")

        except Exception as e:
            logging.error(f"An error occurred while processing {table_names[file_index]}: {str(e)}")

        # Increments index for the next file_name and table_name.
        file_index += 1

    conn.close()
    
if __name__ == '__main__':
    main()
