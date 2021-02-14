curl -X GET 'localhost:3002/api/as/v1/engines/ml-recipes/logs/api' \
-H 'Content-Type: application/json' \
-H 'Authorization: Bearer private-ea3kbk4jaarxhhagneqsgm9m' \
-d '{
  "filters": {
    "date": {
      "from": "2021-01-15T00:00:00+00:00",
      "to": "2021-02-16T00:00:00+00:00"
    }
  },
  "page": {
    "total_results": 1000,
    "size": 100
  },
  "query": "",
  "sort_direction": "desc"
}'