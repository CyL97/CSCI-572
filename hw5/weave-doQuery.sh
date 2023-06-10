echo '{
  "query": "{
    Get{
      SimSearch (
        limit: 3
        nearText: {
          concepts: [\"computer science\"],
        }
      ){
        coursename
        coursedesc
        school
      }
    }
  }"
}'  | curl -X POST -H 'Content-Type: application/json' -d @- localhost:8080/v1/graphql 
