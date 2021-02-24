from elasticsearch_dsl import analyzer, Date, Document, Index, Text, Integer, Keyword, Double

class Listing(Document):
    id = Integer()
    listing_url = Text()
    scrape_id = Integer()
    last_scraped = Keyword()
    crawled_date = Date()
    name = Text(analyzer='snowball')
    host_id = Integer()
    host_is_superhost = Keyword()
    host_identity_verified = Text(fields={'raw': Keyword()})
    room_type = Text(fields={'raw': Keyword()})
    accommodates = Integer()
    guests_included = Integer()
    minimum_nights = Integer()
    maximum_nights = Integer()
    calendar_updated = Text(fields={'raw': Keyword()})
    instant_bookable = Keyword()
    is_business_travel_ready = Keyword()
    cancellation_policy = Text(fields={'raw': Keyword()})
    price = Integer()
    availability_30 = Integer()
    availability_60 = Integer()
    availability_90 = Integer()
    availability_365 = Integer()
    number_of_reviews = Integer()
    first_review = Text(fields={'raw': Keyword()})
    last_review = Text(fields={'raw': Keyword()})
    review_scores_rating = Integer()
    review_scores_accuracy = Integer()
    review_scores_cleanliness = Integer()
    review_scores_checkin = Integer()
    review_scores_communication = Integer()
    review_scores_location = Integer()
    review_scores_value = Integer()
    overall_rating = Double()