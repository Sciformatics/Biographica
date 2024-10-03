#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#                                          MODULES

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from alex_logger import logger
from alex_constants import *
from bs4 import BeautifulSoup
import subprocess, requests, time
import pyranges as pr
import pandas as pd

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#                         TEST INPUT - ORGANISM & RELEASE NUMBER OF FILE

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

organism = 'arabidopsis_lyrata'
release_number = 'release-21'

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#                                          EXTRACT

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#USER DEFINED INPUTS FOR ORGANISM AND RELEASE

# def define_input_data():
#     input1 = str(input('(lowercase) Enter an organism name: '))
#     input2 = str(input('Enter a release version: '))
#     organism = input1
#     release_number=input2
#     return organism, release_number

#PARSE HTML TO FIND FILE TO DOWNLOAD

def find_file(organism, release_number):
    organism_file_dict = {}
    url = f'https://ftp.ensemblgenomes.ebi.ac.uk/pub/plants/{release_number}/gtf/{organism}/'
    logger(f'parsing response to {url}')
    response = requests.get(url)
    logger(f'Status code: {response.status_code}')
    logger('Getting html tags to identify gtf file...')
    soup = BeautifulSoup(response.text, 'html.parser')
    organism_file_dict[organism] = [tag.get('href') for tag in soup.find_all('a') if (organism.capitalize() and 'gtf.gz' in tag.get('href'))][0]
    logger('File identified')
    return organism_file_dict, url

#USE CLI CURL and GUNZIP COMMANDS TO DOWNLOAD AND UNZIP GTF.GZ FILE

def download_and_unzip(organism_file_dict, url):
    file_name = organism_file_dict[organism]
    downloads_file_path = f'{DOWNLOADS_DIR}{organism_file_dict[organism]}'
    logger(f'Downloading file {file_name} from the web...')
    download_cmd = subprocess.run(['curl', '-L', '-o', downloads_file_path, f'{url}{file_name}'])
    logger(f'Response code: {download_cmd.returncode}')
    time.sleep(3)
    logger('Unzipping file...')
    unzip_cmd = subprocess.run(['gunzip', f'{downloads_file_path}'])
    logger('File unzipped!')
    return downloads_file_path

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#                          TRANSFORMATIONS - CREATE BASE AND METADATA DATAFRAMES

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#CREATE A BASE DF OF THE PARSED FILE OUTPUT
#ADD PRIMARY KEY TO IMPART ROW UNIQUENESS


def create_base_df(organism, organism_file_dict, url, downloads_file_path, release_number):
    unzipped_extension = downloads_file_path.strip().split(DOWNLOADS_DIR)[1].split('.gz')[0]
    unzipped_file = DOWNLOADS_DIR+unzipped_extension

    #create dataframe, add primary key to impart row uniqueness
    logger('Reading in and parsing file into dataframe...')
    base_df = pr.read_gtf(unzipped_file)
    logger('Dataframe built!')
    base_df = base_df.df
    logger('Building primary key...')
    primary_key = [f"{unzipped_extension}#{str(row['Chromosome'])}#{str(row['Source'])}#{str(row['Feature'])}#{str(row['Start'])}#{str(row['Strand'])}" for _, row in base_df.iterrows()]
    base_df.insert(0, 'primary_key', primary_key)
    logger('Saving file as an output .CSV...')
    base_df.to_csv(f'{DATA_DIR}{organism}_{release_number}_base_df.csv')
    logger('File saved!')
    return base_df, unzipped_extension

#CREATE A METADATA DF OF PARSED FILE OUTPUT

def create_metadata_df(base_df, unzipped_extension, release_number):
    num_cols = len(base_df.columns)
    num_rows = len(base_df)
    missingness = round((base_df.isna().sum().sum()/(base_df.shape[0]*base_df.shape[1]))*100, 2)
    unique_protein_ids = len(set(base_df['protein_id']))
    unique_gene_ids = len(set(base_df['gene_id']))
    logger('Building the metadata dataframe...')
    metadata_df = pd.DataFrame({
        'organism_gtf':unzipped_extension,
        'number_of_columns':num_cols,
        'number_of_rows':num_rows,
        'data_missingness(%)':missingness,
        'unique_protein_ids':unique_protein_ids,
        'unique_gene_ids':unique_gene_ids
        }, index=[0])
    logger('Saving file as an output .CSV...')
    metadata_df.to_csv(f'{DATA_DIR}{organism}_{release_number}_metadata_df.csv')
    logger('File saved!')
    return metadata_df

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#                           LOAD - SAVE TO CSV AND TO LOCAL SQL DB FILE

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#INSTANTIATE DB CONNECTION
#LOAD TO A LOCAL DB FILE FOR TESTING

def load_to_sql_db(organism,base_df, metadata_df, release_number):
    logger('Loading dataframes to a local SQL database (.db) file...')
    base_df.to_sql(f'fact_table_{organism}_{release_number}', ENGINE, if_exists='replace')
    metadata_df.to_sql(f'meta_table_{organism}_{release_number}', ENGINE, if_exists='replace')
    logger('Database built!')



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#                                          MAIN

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def main():

    #EXTRACT
    # organism, release_number = define_input_data()
    organism_file_dict, url = find_file(organism, release_number)
    downloads_file_path = download_and_unzip(organism_file_dict, url)

    #TRANSFORM
    base_df, unzipped_extension = create_base_df(organism, organism_file_dict, url, downloads_file_path, release_number)
    metadata_df = create_metadata_df(base_df, unzipped_extension, release_number)

    #LOAD TO LOCAL SQL DB FILE 'TEST_DATABASE.DB'
    load_to_sql_db(organism,base_df, metadata_df, release_number)

if __name__ == "__main__":
    main()