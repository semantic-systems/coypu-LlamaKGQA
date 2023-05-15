import sys
import pandas as pd
from typing import List, Dict
from SPARQLWrapper import SPARQLWrapper, JSON
import pickle
import ipdb
import time
from urllib import request, error
import urllib

class CoypuQueryResults:
    """
    A class that can be used to query data from Coypu graph using SPARQL and return the results as a Pandas DataFrame or a list
    of values for a specific key.
    """
    def __init__(self, query: str):
        """
        Initializes the CoypuQueryResults object with a SPARQL query string.

        :param query: A SPARQL query string.
        """
        self.user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        self.endpoint_url = "https://skynet.coypu.org/coypu-internal/"
        self.sparql = SPARQLWrapper(self.endpoint_url, agent=self.user_agent)
        self.sparql.setCredentials("katherine", "0CyivAlseo")
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)

    def __transform2dicts(self, results: List[Dict]) -> List[Dict]:
        """
        Helper function to transform SPARQL query results into a list of dictionaries.

        :param results: A list of query results returned by SPARQLWrapper.
        :return: A list of dictionaries, where each dictionary represents a result row and has keys corresponding to the
        variables in the SPARQL SELECT clause.
        """
        new_results = []
        for result in results:
            new_result = {}
            for key in result:
                new_result[key] = result[key]['value']
            new_results.append(new_result)
        return new_results

    def _load(self) -> List[Dict]:
        """
        Helper function that loads the data from Wikidata using the SPARQLWrapper library, and transforms the results into
        a list of dictionaries.

        :return: A list of dictionaries, where each dictionary represents a result row and has keys corresponding to the
        variables in the SPARQL SELECT clause.
        """
        try:
            results = self.sparql.queryAndConvert()['results']['bindings']
        except :
            time.sleep(50)
            results = None
        if results:
            results = self.__transform2dicts(results)
        return results

    def load_as_dataframe(self) -> pd.DataFrame:
        """
        Executes the SPARQL query and returns the results as a Pandas DataFrame.

        :return: A Pandas DataFrame representing the query results.
        """
        results = self._load()
        return pd.DataFrame.from_dict(results)

def formQuery(entity:str) :
    query = """
    PREFIX text: <http://jena.apache.org/text#>
    SELECT DISTINCT ?inst ?score WHERE {
    (?inst ?score) text:query ("Macao" 100).
    }
    """.replace("Macao", entity)
    return query

if __name__ ==  "__main__":
    with open ("../ent_det/ner_out.pickle", 'rb') as file:
        file = pickle.load(file)
    for pair in file:   
        ents = pair[1]
        for ent in ents.keys():
            query = formQuery(ent)
            data_extracter = CoypuQueryResults(query)
            df = data_extracter.load_as_dataframe()
            try:
                ids = df["inst"].tolist()[:5]
                pair[1][ent] = [pair[1][ent], ids]
            except:
                pair[1][ent] = [pair[1][ent]]
    with open('el_out.pickle', 'wb') as handle:
        pickle.dump(file, handle, protocol=pickle.HIGHEST_PROTOCOL)
