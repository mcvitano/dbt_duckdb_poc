import pyodbc
import pandas as pd
import argparse
from pathlib import Path
from datetime import date
import warnings



def _read_query_to_parquet(sql, table, connection, output_dir, partition_cols=None):
        df = pd.read_sql(sql=sql, con=connection)

        # add field for dbt freshness alert
        df['LOADED_AT'] = date.today()

        df.to_parquet(
                Path.joinpath(output_dir, Path(table)), 
                engine='pyarrow', 
                compression='snappy', 
                index=False,
                partition_cols = partition_cols)
        

def ingest_raw_data(source, table, current_year_yn):
    # Manually create partitions to avoid reading all data into memory at once
    if current_year_yn == 'N':
        years_list = range(2012, date.today().year + 1)
    else:
        years_list = [date.today().year]
        
    # Create folder for data files
    output_dir = Path.joinpath(
        Path(__file__).parents[0],
        Path('raw_data'), 
        Path(source.replace(' ', '_')))

    output_dir.mkdir(parents=True, exist_ok=True)

    # Ignore pandas warning/preference for SQL Alchemy connections
    warnings.simplefilter(action='ignore', category=UserWarning)

    connection_string = "Driver={SQL Server};Server={VMTEPCLARITY201};Database={CEHDR_Research};trusted_connection=true"
    conn = pyodbc.connect(connection_string)
    print()

    #########################
    # Substance Use Registry
    #
    #########################
    if source == 'substance use registry':
        # Create list of tables
        if not table:
            tables = ['bridge_enrollment', 'diagnoses', 'encounters', 'medications', 'patients']
        else:
            tables = [table]
   
        for table in tables:
            # Bridge Clinic
            if table == 'bridge_enrollment':
                sql="""
                    SELECT PAT_ID, ENROLL_STATUS_NAME, ENROLL_START_DT, ENROLL_END_DT
                    FROM SUBSTANCE_USE_STUDY_ENROLLMENT
                    """
                _read_query_to_parquet(sql=sql, table=table, 
                                       connection=conn, output_dir=output_dir, partition_cols=None)
                print(f'     + {source.replace(' ', '_')}.{table} --> disk')

            # Diagnoses
            if table == 'diagnoses':
                for yr in years_list:
                    # Create directory to hold parquet partitions
                    Path.joinpath(output_dir, Path(f'{table}/YEAR={yr}'))\
                        .mkdir(parents=True, exist_ok=True)
                    sql="""
                        SELECT PAT_ID, PAT_ENC_CSN_ID, 
                            datepart(year, CONTACT_DATE) as YEAR,
                            CONTACT_DATE, CURRENT_ICD10_LIST, DX_NAME
                        FROM SUBSTANCE_USE_DIAGNOSIS_LIST
                        """
                    _read_query_to_parquet(sql=sql, 
                                        table=f'{table}/YEAR={yr}/{table}',
                                        connection=conn,
                                        output_dir=output_dir, 
                                        partition_cols=None)
                    print(f'     + {source.replace(' ', '_')}.{table} --> disk')

            # Encounters
            if table == 'encounters':
                for yr in years_list:
                    # Create directory to hold parquet partitions
                    Path.joinpath(output_dir, Path(f'{table}/YEAR={yr}'))\
                        .mkdir(parents=True, exist_ok=True)
                    
                    sql=f"""
                        SELECT PAT_ID, HSP_ACCOUNT_ID, PAT_ENC_CSN_ID, CONTACT_DATE, 
                            datepart(year, CONTACT_DATE) as YEAR,
                            ENC_TYPE_NM, DEPARTMENT_NAME, DEPARTMENT_SPECIALTY,
                            APPT_TIME, APPT_STATUS_NM, RSN_FOR_VISIT_LIST, OTHER_RSN_LIST,
                            ADT_ARRIVAL_TIME, HOSP_ADMSN_TIME, 
                            INP_ADM_DATE, OP_ADM_DATE, EMER_ADM_DATE, HOSP_DISCH_TIME,
                            TOT_CHGS,
                            VISIT_PROV_ID, PCP_PROV_ID, ADMISSION_PROV_ID, BILL_ATTEND_PROV_ID,
                            PRODUCT_TYPE_NM, PAYOR_NAME
                        FROM SUBSTANCE_USE_ENCOUNTERS
                        WHERE datepart(year, CONTACT_DATE) = {yr}
                        """
                    _read_query_to_parquet(sql=sql, 
                                        table=f'{table}/YEAR={yr}/{table}', 
                                        connection=conn, output_dir=output_dir,
                                        partition_cols=None)
                    print(f'     + {source.replace(' ', '_')}.{table} --> disk')

            # Medications
            if table == 'medications':
                for yr in years_list:
                    # Create directory to hold parquet partitions
                    Path.joinpath(output_dir, Path(f'{table}/YEAR={yr}'))\
                        .mkdir(parents=True, exist_ok=True)
                    
                    sql=f"""
                        SELECT PAT_ID, PAT_ENC_CSN_ID, ORDER_INST, 
                            cast(START_DATE as date) as START_DATE, 
                            cast(END_DATE as date) as END_DATE,
                            DISCON_TIME,
                            ORDERING_MODE_NM, ORDER_STATUS_NM, ORDER_CLASS_NM,
                            ATC_CODE, ATC_TITLE
                        FROM SUBSTANCE_USE_MEDICATIONS
                        WHERE datepart(year, IIF(START_DATE < ORDER_INST, START_DATE, ORDER_INST)) = {yr}
                        """
                    _read_query_to_parquet(sql=sql, 
                                        table=f'{table}/YEAR={yr}/{table}', 
                                        connection=conn, output_dir=output_dir,
                                        partition_cols=None)
                    print(f'     + {source.replace(' ', '_')}.{table} --> disk')

            # Patients
            if table == 'patients':
                sql="""
                    SELECT PAT_ID,  BIRTH_DATE, PAT_SEX, ETHNIC_GROUP_NM, PATIENT_RACE,
                        LANGUAGE_NM, MARITAL_STATUS_NM
                    FROM SUBSTANCE_USE_PATIENTS
                    """
                _read_query_to_parquet(sql=sql, table=table, 
                                       connection=conn, output_dir=output_dir, partition_cols=None)
                print(f'     + {source.replace(' ', '_')}.{table} --> disk')

    #######################
    # National Death Index
    #
    #######################
    if source == 'ndi':
        table = 'ndi'

        # Death Dates
        ndi = pd.read_csv('H:\\Documents\\ndi\\data\\ndi-compiled-manifest.csv', 
                        delimiter='\t',
                        usecols=['PAT_MRN_ID', 'NDI_YEAR_DEATH_FULL', 'NDI_MONTH_DEATH', 'NDI_DAY_DEATH',
                                    'NDI_EXACT_MATCH', 'NDI_PROB_SCORE', 'NDI_STATUS_CODE'],
                        dtype={'PAT_MRN_ID' : int,
                                'NDI_YEAR_DEATH_FULL': str,
                                'NDI_MONTH_DEATH': str,
                                'NDI_DAY_DEATH': str,
                                'NDI_EXACT_MATCH' : str,
                                'NDI_PROB_SCORE' : 'Int64',
                                'NDI_STATUS_CODE': 'Int64'})

        # keep only presumed matches
        ndi = ndi.loc[ndi['NDI_STATUS_CODE'] == 1]

        # zero-pad MRN
        ndi['PAT_MRN_ID'] = ndi['PAT_MRN_ID'].astype(str).str.zfill(7) 

        # keep only highest probability per patient
        ndi.sort_values(['PAT_MRN_ID', 'NDI_PROB_SCORE'], ascending=[True, False], inplace=True)
        ndi.drop_duplicates(subset='PAT_MRN_ID', keep='first',  inplace=True)

        hcv = pd.read_sql(
            sql='SELECT DISTINCT PAT_ID, PAT_MRN_ID FROM HCV_PATIENTS', 
            dtype={'PAT_ID':str, 'PAT_MRN_ID':int}, con=conn)
        hiv = pd.read_sql(
            sql='SELECT DISTINCT PAT_ID, PAT_MRN_ID FROM HISTORICAL_HIV_PATIENTS', 
            dtype={'PAT_ID':str, 'PAT_MRN_ID':int}, con=conn)
        sud = pd.read_sql(
            sql='SELECT DISTINCT PAT_ID, PAT_MRN_ID FROM SUBSTANCE_USE_PATIENTS', 
            dtype={'PAT_ID':str, 'PAT_MRN_ID':int}, con=conn)

        pat_id_mrn_map = pd.concat([hcv, hiv, sud]).drop_duplicates() 
        pat_id_mrn_map['PAT_MRN_ID'] = pat_id_mrn_map['PAT_MRN_ID'].astype(str).str.zfill(7)

        # must be an INNER join ... there are some patients previously matched with the NDI
        #   that are no longer included in a CEHDR registry (changes in specifications)
        ndi = ndi.merge(pat_id_mrn_map, how='inner')

        ndi['NDI_EXACT_MATCH'] = ndi['NDI_EXACT_MATCH'].replace(to_replace='*', value='Y')
        ndi['NDI_EXACT_MATCH'] = ndi['NDI_EXACT_MATCH'].fillna('N')

        # combine to form date field
        ndi['NDI_DEATH_DATE'] = ndi['NDI_YEAR_DEATH_FULL'] + '/' + ndi['NDI_MONTH_DEATH'] + '/' + ndi['NDI_DAY_DEATH']

        ndi = ndi.loc[:, ['PAT_ID', 'NDI_DEATH_DATE', 'NDI_EXACT_MATCH', 'NDI_PROB_SCORE']]
        ndi.drop_duplicates(inplace=True)

        ndi.to_parquet(
                Path.joinpath(output_dir, Path(table)), 
                engine='pyarrow', 
                compression='snappy', 
                index=False)
        
        print(f'     + {source.replace(' ', '_')}.{table} --> disk')


    ##################
    # Global Registry
    #
    ##################
    if source == 'global registry':
        # Create list of tables
        if not table:
            tables = ['pcmh_visits_denominators_2018_2023', 'providers']
        else:
            tables = [table]

        for table in tables:
            # One record per-patient, per-PCMH, per-year (random denominator file from somewhere)
            if table == 'pcmh_visits_denominators_2018_2023':

                pcmh = pd.read_csv('H:\\Documents\\PCMH_Visits_2018_2023_for_NDI_Request_Prep_MAR2024.txt', 
                                delimiter='\t',
                                usecols=['PAT_ID', 'PAT_ENC_CSN_ID', 'CONTACT_DATE', 'DEPARTMENT_NAME',
                                            'HISPANIC_FLAG', 'AFRICAN_AMERICAN_FLAG', 'WHITE_FLAG', 'OTHER_FLAG'],
                                dtype={'PAT_ID':str, 
                                        'PAT_ENC_CSN_ID':int, 
                                        'CONTACT_DATE': str, 
                                        'DEPARTMENT_NAME':str,
                                        'HISPANIC_FLAG':'Int64', 
                                        'AFRICAN_AMERICAN_FLAG':'Int64', 
                                        'WHITE_FLAG':'Int64',
                                        'OTHER_FLAG':'Int64'})
                
                # remove timestamp (00:00:00)
                pcmh['CONTACT_DATE'] = pcmh['CONTACT_DATE'].str.split(' ', expand=True)[0]

                pcmh.loc[pcmh.HISPANIC_FLAG == 1, 'RACETH'] = 'Hispanic'
                pcmh.loc[pcmh.OTHER_FLAG == 1, 'RACETH'] = 'NH Other'
                pcmh.loc[pcmh.AFRICAN_AMERICAN_FLAG == 1, 'RACETH'] = 'NH Black'
                pcmh.loc[pcmh.WHITE_FLAG == 1, 'RACETH'] = 'NH White'
                pcmh.loc[pcmh.RACETH.isnull(), 'RACETH'] = 'Unknown'

                pcmh = pcmh.loc[:, ['PAT_ID', 'PAT_ENC_CSN_ID', 'CONTACT_DATE', 'DEPARTMENT_NAME', 'RACETH']]

                pcmh.to_parquet(
                        Path.joinpath(output_dir, Path(table)), 
                        engine='pyarrow', 
                        compression='snappy', 
                        index=False)
                
                print(f'     + {source.replace(' ', '_')}.{table} --> disk')

            # Providers
            if table == 'providers':
                sql="""
                    SELECT PROV_ID, PROV_TYPE, PROV_NAME, SEX, ACTIVE_STATUS, SPECIALTY_LIST
                    FROM EPIC_PROVIDER_LIST
                    """
                _read_query_to_parquet(sql=sql, table=table, partition_cols=None)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        prog='Ingestion',
        description='Extracts raw data from SQL Server database to parquet file(s).')

    parser.add_argument(
        '-s', '--source', 
        choices=["substance use registry", "global registry", 'ndi'],
        help='Source; Collection of data tables', 
        type=str, 
        required=True)

    parser.add_argument(
        '-t', '--table', 
        help='Table name',
        type=str,
        required=False)

    parser.add_argument(
        '-c', '--current_year_yn', 
        choices=['Y', 'N'],
        help='For tables partitioned by year; Extract only current year?',
        type=str,
        default='Y',
        required=False)

    args = parser.parse_args()

    ingest_raw_data(source=args.source,
                    table=args.table,
                    current_year_yn=args.current_year_yn)