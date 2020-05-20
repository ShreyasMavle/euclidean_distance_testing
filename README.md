# euclidean_distance_testing

Test application to calculate euclidean distance between vectors using Postgresql query and Apache Solr.

Getting started

Create virtual environment:

  On MacOS and Linux:
  python3 -m venv venv

  On Windows:
  py -m venv venv

Install dependencies:
  pip install -r requirements.txt

Getting started with Solr standalone mode:
  
  Download link:
  https://lucene.apache.org/solr/downloads.html
  
  On MacOS and Linux:
  
  Open the terminal and go inside the Solr directory.
  
    Start Solr:
    bin/solr start -p 8983

    Create a Core:
    bin/solr create -c gettingstarted

    Check Solr Status:
    bin/solr status

    Access SolrAdmin UI:
    http://localhost:8983/solr

    Deleting a Core:
    bin/solr delete -c gettingstarted

    Stopping Solr:
    bin/solr stop -p 8983

    Restarting Solr:
    bin/solr restart -p 8983
    
  On Windows:
  
  Open the terminal and go inside the Solr directory.
    
    Start Solr:
    bin\solr start -p 8983

    Create a Core:
    bin\solr create -c gettingstarted

    Check Solr Status:
    bin\solr status

    Access SolrAdmin UI:
    http://localhost:8983/solr

    Deleting a Core:
    bin\solr delete -c gettingstarted

    Stopping Solr:
    bin\solr stop -p 8983

    Restarting Solr:
    bin\solr restart -p 8983
    
To load documents in Solr:
  python euclidean_distance_with_pysolr.py

To load test query performance 
  python pysolr_query_load_testing_performance_using_multiprocessing.py
  
  
  
  
  
  
  
  
  
  
  
  
  
  
