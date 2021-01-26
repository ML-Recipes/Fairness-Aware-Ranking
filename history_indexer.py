
from elasticsearch_dsl import Index, Document, Text, analyzer
from elasticsearch import Elasticsearch, helpers
from airbnb import Listing
import pandas as pd
import logging
import os


from elasticsearch_dsl.connections import connections
es = connections.create_connection(hosts=['localhost'])

def clean_currency(currency):
    price = currency.replace('$', '').replace(',', '')

    return float(price)

def get_overall_rating(row):
    """ Get overall rating using review fields as target indicator """
    review_scores_accuracy = float(row['review_scores_accuracy'])
    review_scores_cleanliness = float(row['review_scores_cleanliness'])
    review_scores_checkin = float(row['review_scores_checkin'])
    review_scores_communication = float(row['review_scores_communication'])
    review_scores_location = float(row['review_scores_location'])
    review_scores_value = float(row['review_scores_value'])

    overall_rating = (((review_scores_accuracy + review_scores_cleanliness \
                      + review_scores_checkin + review_scores_communication \
                      + review_scores_location + review_scores_value) / 2.0) / 6.0)
    return overall_rating

def validate_price(df):
    # Convert 'price' to float
    if('price' in df.columns):
        # Drop rows with null 'price'
        df = df[df['price'].notna()]

        df['price'] = df['price'].apply(clean_currency).astype('float')

    # Handle data with no 'price', e.g., athens_2020-07-21_data_listings.csv.gz
    elif ('price' not in df.columns) & ('weekly_price' in df.columns):
        # Drop rows with null 'weekly_price'
        df = df[df['weekly_price'].notna()]

        df['weekly_price'] = df['weekly_price'].apply(clean_currency).astype('float')
        df['price'] = df['weekly_price'] / 7.0

        #pd.set_option('display.max_columns', None)
        #print(df)
        #exit()
    else:
        df['price'] = None

    return df

def get_features(df):
    """ Select specific columns and convert date columnd to string. """
    
    df = df[
            [ 
                'id', 'listing_url', 'scrape_id', 'last_scraped', 'name', 'host_id', 'price', 
                'availability_30', 'availability_60', 'availability_90', 'availability_365', 
                'number_of_reviews', 'first_review', 'last_review', 'review_scores_rating', 
                'review_scores_accuracy', 'review_scores_cleanliness', 'review_scores_checkin', 
                'review_scores_communication', 'review_scores_location', 'review_scores_value'
            ]
    ]
    
    return df

def drop_no_reviews(df):
    """ Drop no review records. """
    
    df = df[df.first_review.notnull() & df.last_review.notnull() & df.review_scores_rating.notnull() & df.review_scores_accuracy.notnull() & df.review_scores_accuracy.notnull() & df.review_scores_cleanliness.notnull() & df.review_scores_checkin.notnull() & df.review_scores_communication.notnull() & df.review_scores_location.notnull() & df.review_scores_value.notnull()]

    return df

def drop_null_values(df):
    """ Drop records with NaN values. """
    
    df = df.dropna()

    return df

def ingest_data(df, index):
    """ Finalize data and ingest a bulk of documents to ES index """

    try:
        docs = []
        for _, row in df.iterrows():
            doc = Listing()
            overall_rating = get_overall_rating(row)
            
            if 'id' in row:
                doc.id = row['id']
            if 'listing_url' in row:
                doc.listing_url = row['listing_url']
            if 'scrape_id' in row:
                doc.scrape_id = row['scrape_id']
            if 'last_scraped' in row:
                doc.last_scraped = str(row['last_scraped']).replace("-", "")
            if 'name' in row:
                doc.name = row['name']
            if 'host_id' in row:
                doc.host_id = row['host_id']
            if 'price' in row:
                doc.price = row['price']
            if 'availability_30' in row:
                doc.availability_30 = row['availability_30']
            if 'availability_60' in row:
                doc.availability_60 = row['availability_60']
            if 'availability_90' in row:
                doc.availability_90 = row['availability_90']
            if 'availability_365' in row:
                doc.availability_365 = row['availability_365']
            if 'number_of_reviews' in row:
                doc.number_of_reviews = row['number_of_reviews']
            if 'first_review' in row:
                doc.first_review = str(row['first_review']).replace("-", "")
            if 'last_review' in row:
                doc.last_review = str(row['last_review']).replace("-", "")
            if 'review_scores_rating' in row:
                doc.review_scores_rating = row['review_scores_rating']
            if 'review_scores_accuracy' in row:
                doc.review_scores_accuracy = row['review_scores_accuracy']
            if 'review_scores_cleanliness' in row:
                doc.review_scores_cleanliness = row['review_scores_cleanliness']
            if 'review_scores_checkin' in row:
                doc.review_scores_checkin = row['review_scores_checkin']
            if 'review_scores_communication' in row:
                doc.review_scores_communication = row['review_scores_communication']
            if 'review_scores_location' in row:
                doc.review_scores_location = row['review_scores_location']
            if 'review_scores_value' in row:
                doc.review_scores_value = row['review_scores_value']
          
            doc.overall_rating = overall_rating

            docs.append(doc.to_dict(include_meta=False))

        response = helpers.bulk(es, actions=docs, index=index, doc_type='doc')
            
    except Exception:
        logging.error('exception occured', exc_info=True)


if __name__ == "__main__":
    try:

        unique_list = [] 

        print("Start indexing ...")

        path = '/Users/daciantamasan/Desktop/WayBack_InsideAirBNB/'

        for file in sorted(os.listdir(path)):
            if file.endswith(".csv.gz"):
                # Start from the last check point
                #if file <= 'crete_2019-02-16_data_listings.csv.gz':
                #if file <= 'munich_2019-04-17_data_listings.csv.gz':           // 0 file size
                # Extract city name
                name = file.find("_")
                city = file[0:name].lower()
                
                # Load original listing data
                df = pd.read_csv(path + file, compression='gzip')
                
                # Pre-process raw data
                # Step 1: Enrich raw data with price
                df = validate_price(df)
                raw_count = len(df)
                
                # Step 2: Drop records with no reviews
                df = get_features(df)
                df = drop_no_reviews(df)
                review_count = len(df)
                
                # Step 3: Drop records with null values
                df = drop_null_values(df)
                final_count = len(df)
                
                # Obtain the index name
                index_name = 'airbnb_history_' + city

                # Check if the city is seen for the first time 
                if index_name not in unique_list:

                    print("\tNew index %s with %d orig. rows, %d rows with reviews, %d final rows" % (file, raw_count, review_count, final_count))
                    
                    unique_list.append(index_name)
                
                    # Initialize index (only perform once)
                    index = Index(index_name)

                    # Define custom settings
                    index.settings(
                        number_of_shards=1,
                        number_of_replicas=0
                    )

                    # Delete the index, ignore if it doesn't exist
                    index.delete(ignore=404)

                    # Create the index in elasticsearch
                    index.create()

                    # Register a document with the index
                    index.document(Listing)

                else:
                    print("\tOld index %s with %d orig. rows, %d rows with reviews, %d final rows" % (file, raw_count, review_count, final_count))

                ingest_data(df, index=index_name)
                
        print("Finished indexing ...")
    
    except Exception:
        logging.error('exception occured', exc_info=True)
    
    
   

