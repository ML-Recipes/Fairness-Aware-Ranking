from datetime import datetime
from elasticsearch import Elasticsearch


class Searcher(object):
    """
    Class for searching results using parameters
    
    :param location: location string
    :param start_date: start date calendar
    :param end_date: end date calendar
    :param k: total num of documents to retrieve
    """
    def __init__(self, location, start_date='2020-12-21', end_date='2021-03-21', k=300):
        
        self.k = k
        self.start_date = start_date
        self.end_date = end_date
        self.index_name = "airbnb_" + location
        
        self.num_hits = 0
        self.es = Elasticsearch()

    def get_all_docs(self):
        """ Get a list of documents ordered by id """
        res = self.es.search(
            index=self.index_name, 
            body={
                "size": self.k,
                "query": {
                    "match_all": {}
                }
            }
        )

        self.num_hits = res['hits']['total']['value']
        docs = res['hits']['hits']
        return docs

    def get_docs_ord_by_overall_rating(self):
        """ Get a list of documents in descending order by overall rating """ 
        res = self.es.search(
            index=self.index_name, 
            body={"size": self.k, "sort" : [{ "overall_rating" : "desc" }, "_score"], "query": {"match_all": {}}}
        )

        self.num_hits = res['hits']['total']['value']
        docs = res['hits']['hits']
        return docs

    
    def get_docs_by_availability30(self):
        """ Get a list of documents by availability 30 """
        res = self.es.search(index=self.index_name, body={
            "size": self.k, 
            "sort" : [
                { 
                    "overall_rating" : "desc" 
                }, "_score"
            ], 
            "query": {
                "bool": {  
                    "filter": [ 
                        { "range": { 
                            "availability_30": { 
                                "gt": 0 
                            }
                        }
                    }
                ]}
            }
        })
        
        self.num_hits = res['hits']['total']['value']
        docs = res['hits']['hits']
        return docs


if __name__ == "__main__":
    s = Searcher(location='boston')
    
    # Ranker 1. Return all documents and order by id
    docs = s.get_all_docs()
    hits = s.num_hits
    print("Total num docs ordered by id: ", hits)
    print("/n")
    
    # Ranker 2. Return all documents and order descendingly by overall_rating, id
    docs = s.get_docs_ord_by_overall_rating()
    hits = s.num_hits
    print("Total num docs ordered by overall_rating, id: ", hits)
    print("/n")
    
    # Ranker 3. Return all documents with availability > 0 and order descendingly by overall_rating, id
    docs = s.get_docs_by_availability30()
    hits = s.num_hits
    print("Total num docs by availability30: ", hits)
   
    
