from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk
import pandas as pd
from tqdm import tqdm 
import logging

es = connections.create_connection(hosts=['localhost'])

class Indexer():
    def save_docs(self, docs, index):
        if not es.indices.exists(index):
           es.indices.create(index)

        try:
        
            print("Attempting to index the list of docs using helpers.bulk()")
            bulk(es, docs, index=index, chunk_size=100, request_timeout=20)
        except Exception as e:
            print(e)
            print("/n")
            print("index: ", index)
            exit()


def get_overall_rating(row):
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


def get_docs(df):
    """ Get list of documents for a particular dataframe. """
    docs = []

    for _, row in df.iterrows():

        overall_rating = get_overall_rating(row)

        doc = {
                '_id': row['id'],
                'listing_url': row['listing_url'],
                'scrape_id': row['scrape_id'],
                'last_scraped': row['last_scraped'],
                'name': row['name'],
                'description': row['description'],
                'neighborhood_overview': row['neighborhood_overview'],
                'picture_url': row['picture_url'],
                'host_id': row['host_id'],
                'host_url': row['host_url'],
                'host_name': row['host_name'],
                'host_since': row['host_since'],
                'host_location': row['host_location'],
                'host_about': row['host_about'],
                'host_response_time': row['host_response_time'],
                'host_response_rate': row['host_response_rate'],
                'host_acceptance_rate': row['host_acceptance_rate'],
                'host_is_superhost': row['host_is_superhost'],
                'host_thumbnail_url': row['host_thumbnail_url'],
                'host_picture_url': row['host_picture_url'],
                'host_neighbourhood': row['host_neighbourhood'],
                'host_listings_count': row['host_listings_count'],
                'host_total_listings_count': row['host_total_listings_count'],
                'host_verifications': row['host_verifications'],
                'host_has_profile_pic': row['host_has_profile_pic'],
                'host_identity_verified': row['host_identity_verified'],
                'neighbourhood': row['neighbourhood'],
                'neighbourhood_cleansed': row['neighbourhood_cleansed'],
                #'neighbourhood_group_cleansed': row['neighbourhood_group_cleansed'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'property_type': row['property_type'],
                'room_type': row['room_type'],
                'accommodates': row['accommodates'],
                #'bathrooms': row['bathrooms'],
                'bathrooms_text': row['bathrooms_text'],
                'bedrooms': row['bedrooms'],
                'beds': row['beds'],
                'amenities': row['amenities'],
                'price': row['price'],
                'minimum_nights': row['minimum_nights'],
                'maximum_nights': row['maximum_nights'],
                'minimum_minimum_nights': row['minimum_minimum_nights'],
                'maximum_minimum_nights': row['maximum_minimum_nights'],
                'minimum_maximum_nights': row['minimum_maximum_nights'],
                'maximum_maximum_nights': row['maximum_maximum_nights'],
                'minimum_nights_avg_ntm': row['minimum_nights_avg_ntm'],
                'maximum_nights_avg_ntm': row['maximum_nights_avg_ntm'],
                #'calendar_updated': row['calendar_updated'],
                'has_availability': row['has_availability'],
                'availability_30': row['availability_30'],
                'availability_60': row['availability_60'],
                'availability_90': row['availability_90'],
                'availability_365': row['availability_365'],
                'calendar_last_scraped': row['calendar_last_scraped'],
                'number_of_reviews': row['number_of_reviews'],
                'number_of_reviews_ltm': row['number_of_reviews_ltm'],
                'number_of_reviews_l30d': row['number_of_reviews_l30d'],
                'first_review': row['first_review'],
                'last_review' : row['last_review'],
                'review_scores_rating': row['review_scores_rating'],
                'review_scores_accuracy': row['review_scores_accuracy'],
                'review_scores_cleanliness': row['review_scores_cleanliness'],
                'review_scores_checkin': row['review_scores_checkin'],
                'review_scores_communication': row['review_scores_communication'],
                'review_scores_location': row['review_scores_location'],
                'review_scores_value': row['review_scores_value'],
                #'license': row['license'],
                'instant_bookable': row['instant_bookable'],
                'calculated_host_listings_count': row['calculated_host_listings_count'],
                'calculated_host_listings_count_entire_homes': row['calculated_host_listings_count_entire_homes'],
                'calculated_host_listings_count_private_rooms': row['calculated_host_listings_count_private_rooms'],
                'calculated_host_listings_count_shared_rooms': row['calculated_host_listings_count_shared_rooms'],
                'reviews_per_month': row['reviews_per_month'],
                'overall_rating': overall_rating
            }

        docs.append(doc)

    return docs


if __name__ == "__main__":

    try:

        print("Start indexing ...")

        for city in ['Boston', 'Geneva', 'Hong_Kong']:
            # get documents from DataFrame
            df = pd.read_csv('data/Airbnb/' + city + '/clean_listings.csv')
            docs = get_docs(df)
            # create handler and save documents to ES 
            indexer = Indexer()
            indexer.save_docs(docs, index='airbnb_' + city.lower())
        
        print("Finished indexing ...")

    
    except Exception:
        logging.error('exception occured', exc_info=True)


    



   








