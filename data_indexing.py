from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, Nested, Double
from elasticsearch_dsl.connections import connections
import pandas as pd
from tqdm import tqdm 
import logging

connections.create_connection(hosts=['localhost'])

# Define a default Elasticsearch client
class AirbnbDocument(Document):
    
    id = Integer()
    listing_url = Text()
    scrape_id = Text()
    last_scraped = Date()
    name = Text()
    description = Text() 
    neighborhood_overview = Text()
    picture_url = Text()
    host_id = Integer()
    host_url = Text()
    host_name = Text()
    host_since = Date()
    host_location = Text()
    host_about = Text()
    host_response_time = Text()
    host_response_rate = Text()
    host_acceptance_rate = Text()
    host_is_superhost = Text()
    host_thumbnail_url = Text()
    host_picture_url = Text()
    host_neighbourhood = Text()
    host_listings_count = Integer()
    host_total_listings_count = Integer()
    host_verifications = Text(fields={'keyword': Keyword()})
    host_has_profile_pic = Text()
    host_identity_verified = Text()
    neighbourhood = Text()
    neighbourhood_cleansed = Text()
    neighbourhood_group_cleansed = Text()
    latitude = Double()
    longitude = Double()
    property_type = Text()
    room_type = Text()
    accommodates = Integer()
    bathrooms = Text()
    bathrooms_text = Text()
    bedrooms = Text()
    beds = Integer()
    amenities = Text(fields={'keyword': Keyword()})
    price = Text()
    minimum_nights = Integer()
    maximum_nights = Integer()
    
    class Index:
        name = 'airbnb'

    def save(self, ** kwargs):
        return super(AirbnbDocument, self).save(** kwargs)

    def get_listing_url(self, listing_url):
        """ Get listing_url data"""
        result = ""
        if isinstance(listing_url, str):
            result = listing_url
        else:
            result = "NaN"
        return result
    
    def get_host_about(self, host_about):
        """ Get host_about data"""
        result = ""
        if isinstance(host_about, str):
            result = host_about
        else:
            result = "NaN"
        return result


if __name__ == "__main__":
    
   
    df = pd.read_csv('data/airbnb.csv')
    
    try:

        # create mappings & index using Elasticsearch-DSL
        AirbnbDocument.init()

        # loop through all rows in DataFrame and convert to right data type
        for index, row in tqdm(df.iterrows()):
            
            a = AirbnbDocument()
            a.meta.id = row['id']
            a.listing_url = str(row['listing_url'])
            a.scrape_id = str(row['scrape_id'])
            a.last_scraped = row['last_scraped']
            a.name = str(row['name'])
            a.description = str(row['description'])
            a.neighborhood_overview = str(row['neighborhood_overview'])
            a.picture_url = str(row['picture_url'])
            a.host_id = row['host_id']
            a.host_url = str(row['host_url'])
            a.host_name = str(row['host_name'])
            a.host_since = row['host_since']
            a.host_location = str(row['host_location'])
            a.host_about = str(row['host_about'])
            a.host_response_time = str(row['host_response_time'])
            a.host_response_rate = str(row['host_response_rate'])
            a.host_acceptance_rate = str(row['host_acceptance_rate'])
            a.host_is_superhost = str(row['host_is_superhost'])
            a.host_thumbnail_url = str(row['host_thumbnail_url'])
            a.host_picture_url = str(row['host_picture_url'])
            a.host_neighbourhood = str(row['host_neighbourhood'])
            a.host_listings_count = int(row['host_listings_count'])
            a.host_total_listings_count = int(row['host_total_listings_count'])
            a.host_verifications = row['host_verifications']
            a.host_has_profile_pic = str(row['host_has_profile_pic'])
            a.host_identity_verified = str(row['host_identity_verified'])
            a.neighbourhood = str(row['neighbourhood'])
            a.neighbourhood_cleansed = str(row['neighbourhood_cleansed'])
            a.neighbourhood_group_cleansed = str(row['neighbourhood_group_cleansed'])
            a.latitude = float(row['latitude'])
            a.longitude = float(row['longitude'])
            a.property_type = str(row['property_type'])
            a.room_type = str(row['room_type'])
            a.accommodates = int(row['accommodates'])
            
            a.save()
            
            
            
            """
            a.bathrooms = row['bathrooms']
            a.bathrooms_text = str(row['bathrooms_text'])
            a.bedrooms = int(row['bedrooms'])
            a.beds = int(row['beds'])
            a.amenities = row['amenities']
            a.price = str(row['price'])
            a.minimum_nights = int(row['minimum_nights'])
            a.maximum_nights = int(row['maximum_nights'])
            
            a.minimum_minimum_nights = int(row['minimum_minimum_nights'])
            a.maximum_minimum_nights = int(row['maximum_minimum_nights'])
            a.minimum_maximum_nights = int(row['minimum_maximum_nights'])
            a.maximum_maximum_nights = int(row['maximum_maximum_nights'])
            a.minimum_nights_avg_ntm = int(row['minimum_nights_avg_ntm'])
            a.maximum_nights_avg_ntm = int(row['maximum_nights_avg_ntm'])
            a.calendar_updated = int(row['calendar_updated'])
            a.has_availability = int(row['has_availability'])
            a.availability_30 = int(row['availability_30'])
            a.availability_60 = int(row['availability_60'])
            a.availability_90 = int(row['availability_90'])
            a.availability_365 = int(row['availability_365'])
            a.calendar_last_scraped = int(row['calendar_last_scraped'])
            a.number_of_reviews = int(row['number_of_reviews'])
            a.number_of_reviews_ltm = int(row['number_of_reviews_ltm'])
            a.number_of_reviews_l30d = int(row['number_of_reviews_l30d'])
            a.first_review = int(row['first_review'])
            a.last_review = int(row['last_review'])
            a.review_scores_rating = int(row['review_scores_rating'])
            a.review_scores_accuracy = int(row['review_scores_accuracy'])
            a.review_scores_cleanliness = int(row['review_scores_cleanliness'])
            a.review_scores_checkin = int(row['review_scores_checkin'])
            a.review_scores_communication = row['review_scores_communication']
            a.review_scores_location = row['review_scores_location']
            a.review_scores_value = row['review_scores_value']
            a.license = row['license']
            a.instant_bookable = row['instant_bookable']
            a.calculated_host_listings_count = row['calculated_host_listings_count']
            a.calculated_host_listings_count_entire_homes = row['calculated_host_listings_count_entire_homes']
            a.calculated_host_listings_count_private_rooms = row['calculated_host_listings_count_private_rooms']
            a.calculated_host_listings_count_shared_rooms = row['calculated_host_listings_count_shared_rooms']
            a.reviews_per_month = row['reviews_per_month']
            """
            
            
        
    except Exception:
        logging.error("exception occured", exc_info=True)



