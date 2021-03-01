
from elasticsearch_dsl import analyzer, Date, Document, Index, Text, Integer, Keyword, Double
from elasticsearch import Elasticsearch, helpers
from airbnb import Listing
import pandas as pd
import logging
import os


from elasticsearch_dsl.connections import connections
es = connections.create_connection(hosts=['localhost'])

def clean_currency(price):
    
    if '$' in price:
        price = price.replace('$', '')

    if ',' in price:
        price = price.replace(',', '')

    return price

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
        # Fill rows with null 'price'
        df['price'].fillna(value='0', inplace=True)

        df['price'] = df['price'].apply(clean_currency).astype('float')

    # Handle data with no 'price', e.g., athens_2020-07-21_data_listings.csv.gz
    elif ('price' not in df.columns):
        
        if ('weekly_price' in df.columns):

            # Fill rows with null 'weekly_price'
            df['weekly_price'].fillna(value='0', inplace=True)
            
            df['weekly_price'] = df['weekly_price'].apply(clean_currency).astype('float')
            df['price'] = df['weekly_price'] / 7.0

        else:
            
            # Set missigng 'price' and 'weekly_price' to 0
            df['price'].fillna(value='0', inplace=True)
            df['weekly_price'].fillna(value='0', inplace=True)

    return df

def get_crawled_date(df):
    """ Extract crawled date from the 'scrape_id' field. """

    df['crawled_date'] = df['scrape_id'].astype(str)
    df['crawled_date'] = df['crawled_date'].apply(lambda x: x[:8])
    
    return df

def gen_missing_columns(df):
    """ Extract 'guests_included' from the 'accommodates' field. """

    if 'host_is_superhost' not in df.columns:
        df['host_is_superhost'] = "f"

    if 'host_identity_verified' not in df.columns:
        df['host_identity_verified'] = "f"

    if 'room_type' not in df.columns:
        df['room_type'] = "n/a"

    if 'accommodates' not in df.columns:
        df['accommodates'] = 0

    if 'guests_included' not in df.columns:
        df['guests_included'] = df['accommodates']

    if 'minimum_nights' not in df.columns:
        df['minimum_nights'] = 0

    if 'maximum_nights' not in df.columns:
        df['maximum_nights'] = 0

    if 'calendar_updated' not in df.columns:
        df['calendar_updated'] = "n/a"

    if 'instant_bookable' not in df.columns:
        df['instant_bookable'] = "f"

    if 'is_business_travel_ready' not in df.columns:
        df['is_business_travel_ready'] = "f"

    if 'cancellation_policy' not in df.columns:
        df['cancellation_policy'] = "n/a"

    return df

def get_features(df):
    """ Select specific columns and convert date columnd to string. """
    
    df = df[
            [ 
                'id', 'listing_url', 'scrape_id', 'last_scraped', 'crawled_date', 
                'name', 'host_id', 'host_is_superhost', 'host_identity_verified', 
                'room_type', 'accommodates', 'guests_included','minimum_nights', 
                'maximum_nights', 'calendar_updated', 'instant_bookable', 'is_business_travel_ready', 'cancellation_policy',
                'price', 'availability_30', 'availability_60', 'availability_90', 'availability_365', 
                'number_of_reviews', 'first_review', 'last_review', 'review_scores_rating', 
                'review_scores_accuracy', 'review_scores_cleanliness', 'review_scores_checkin', 
                'review_scores_communication', 'review_scores_location', 'review_scores_value'
            ]
    ]
    
    return df

def validate_reviews(df):
    """ Enrich no review records with default review scores. """
    
    df['first_review'].fillna(value='1991-01-01', inplace=True)
    df['last_review'].fillna(value='0', inplace=True)
    df['review_scores_rating'].fillna(value=0, inplace=True)
    df['review_scores_accuracy'].fillna(value=0, inplace=True)
    df['review_scores_accuracy'].fillna(value=0, inplace=True)
    df['review_scores_cleanliness'].fillna(value=0, inplace=True)
    df['review_scores_checkin'].fillna(value=0, inplace=True)
    df['review_scores_communication'].fillna(value=0, inplace=True)
    df['review_scores_location'].fillna(value=0, inplace=True)
    df['review_scores_value'].fillna(value=0, inplace=True)

    return df

def drop_null_values(df):
    """ Drop records with NaN values. """
    
    df = df.dropna()

    return df

def fill_null_values(df):
    """ Fill records with NaN values. """
    
    df['listing_url'].fillna(value=' ', inplace=True)
    df['scrape_id'].fillna(value=0, inplace=True)
    df['last_scraped'].fillna(value='1991-01-01', inplace=True)
    df['crawled_date'].fillna(value='1991-01-01', inplace=True)
    df['name'].fillna(value=' ', inplace=True)
    df['host_id'].fillna(value=0, inplace=True)
    df['host_is_superhost'].fillna(value=' ', inplace=True)
    df['host_identity_verified'].fillna(value=' ', inplace=True)
    df['room_type'].fillna(value=' ', inplace=True)
    df['accommodates'].fillna(value=0, inplace=True)
    df['guests_included'].fillna(value=0, inplace=True)
    df['minimum_nights'].fillna(value=0, inplace=True)
    df['maximum_nights'].fillna(value=0, inplace=True)
    df['calendar_updated'].fillna(value=' ', inplace=True)
    df['instant_bookable'].fillna(value=' ', inplace=True)
    df['is_business_travel_ready'].fillna(value=' ', inplace=True)
    df['cancellation_policy'].fillna(value=' ', inplace=True)
    df['price'].fillna(value=0, inplace=True)
    df['availability_30'].fillna(value=0, inplace=True)
    df['availability_60'].fillna(value=0, inplace=True)
    df['availability_90'].fillna(value=0, inplace=True)
    df['availability_365'].fillna(value=0, inplace=True)
    df['number_of_reviews'].fillna(value=0, inplace=True)

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
            if 'crawled_date' in row:
                doc.crawled_date = row['crawled_date']
            if 'name' in row:
                doc.name = row['name']
            if 'host_id' in row:
                doc.host_id = row['host_id']
            if 'host_is_superhost' in row:
                doc.host_is_superhost = row['host_is_superhost']
            if 'host_identity_verified'in row:
                doc.host_identity_verified = row['host_identity_verified']
            if 'room_type' in row:
                doc.room_type = row['room_type']
            if 'accommodates' in row:
                doc.accommodates = row['accommodates']
            if 'guests_included' in row:
                doc.guests_included = row['guests_included']
            if 'minimum_nights' in row:
                doc.minimum_nights = row['minimum_nights']
            if 'maximum_nights' in row:
                doc.maximum_nights = row['maximum_nights']
            if 'calendar_updated' in row:
                doc.calendar_updated = row['calendar_updated']
            if 'instant_bookable' in row:
                doc.instant_bookable = row['instant_bookable']
            if 'is_business_travel_ready' in row:
                doc.is_business_travel_ready = row['is_business_travel_ready']
            if 'cancellation_policy' in row:
                doc.cancellation_policy = row['cancellation_policy']
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

        path = '/Users/nattiya/Desktop/WayBack_InsideAirBNB/'

        for file in sorted(os.listdir(path)):
            # Index cities reported in ACL'2020 paper
            # if (file.startswith("boston") or file.startswith("geneva") or file.startswith("hong-kong"))and file.endswith(".csv.gz"):

            # Top 10 cities by active listings (https://www.alltherooms.com/analytics/airbnb-statistics/):
            #if (file.startswith("london") or file.startswith("paris") or file.startswith("new-york-city") or file.startswith("rome") or file.startswith("rio-de-janeiro") or file.startswith("buenos-aires") or file.startswith("sydney") or file.startswith("mexico-city") or file.startswith("barcelona")) and file.endswith(".csv.gz"):
            if (file.startswith("london") or file.startswith("barcelona")) and (("2019-" in file) or ("2020-" in file)) and file.endswith(".csv.gz"):
            
                # Start from the last check point
                #if file <= 'crete_2019-02-16_data_listings.csv.gz':
                #if file <= 'munich_2019-04-17_data_listings.csv.gz':           // 0 file size
                #if file < 'boston_2020-07-11_data_listings.csv.gz':

                # Extract city name
                name = file.find("_")
                city = file[0:name].lower()
                
                # Load original listing data
                df = pd.read_csv(path + file, compression='gzip')
                
                # Pre-process raw data
                # Step 1: Enrich raw data with price and crawled date
                df = validate_price(df)
                df = get_crawled_date(df)
                df = gen_missing_columns(df)
                raw_count = len(df)

                # Step 2: Assign ratings to listings with no reviews
                df = get_features(df)
                df = validate_reviews(df)
                review_count = len(df)
                
                # Step 3: Drop records with null values
                #df = drop_null_values(df)
                df = fill_null_values(df)
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
    
    
   

