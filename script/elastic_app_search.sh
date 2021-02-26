curl -X GET 'localhost:3002/api/as/v1/engines/airbnb-history-boston/logs/api' \
-H 'Content-Type: application/json' \
-H 'Authorization: Bearer private-6jj3ai4ckkq2xykcocosmv6o' \
-d '{
  "filters": {
    "date": {
      "from": "2021-02-20T00:00:00+00:00",
      "to": "2021-02-26T00:00:00+00:00"
    }
  },
  "page": {
    "total_results": 100,
    "size": 20
  }
}'