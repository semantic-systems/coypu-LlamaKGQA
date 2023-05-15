
from typing import List, Tuple, Optional
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib
import yaml

class KnowledgeGraphCoyPu(object):

    def __init__(self, endpoint: str) -> None:
        self.sparql = SPARQLWrapper(endpoint)
        self.sparql.setReturnFormat(JSON)

    def setCredentials(self, user: Optional[str], password: Optional[str]) -> None:
        self.sparql.setCredentials(user, password)

    def get_relations(self, entity: str, limit: int = 1000) -> List[str]:
        """
        get all the relations for a given source entity.
        """
        query = f"""
                SELECT DISTINCT ?r0 WHERE {{
                    <{entity}> ?r0 ?t0 .
                }} LIMIT {limit}
        """
        self.sparql.setQuery(query)
        try:
            results = self.sparql.query().convert()
        except urllib.error.URLError:
            print(query)
            exit(0)

        rnt = []

        for i in results['results']['bindings']:
            if i['r0']['value'] != "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" and \
                    i['r0']['value'] != "http://www.w3.org/2000/01/rdf-schema#label":
                rnt.append(i['r0']['value'])

        return rnt


    def get_tails(self, src: str, relation: str) -> List[str]:
        """
        get all tails for a given source entity and relation.
        """
        query = f"""
                SELECT DISTINCT ?t0 WHERE {{
                    <{src}> <{relation}> ?t0 .
                }}
        """
        self.sparql.setQuery(query)
        try:
            results = self.sparql.query().convert()
        except urllib.error.URLError:
            print(query)
            exit(0)
        return [i['t0']['value'] for i in results['results']['bindings']]


    def get_one_hop_paths(self, src: str) -> List[Tuple[str, str, str]]:
        """
        retrieve all one-hop paths as a list of tuple (source, predicate, target) for a given source entity.
        """
        query = f"""
                SELECT DISTINCT ?r0 ?t0 WHERE {{
                    <{src}> ?r0 ?t0 .
                }}
                """
        self.sparql.setQuery(query)
        try:
            results = self.sparql.query().convert()
        except Exception as e:
            print(query)
            exit(0)

        rnt = []

        for i in results['results']['bindings']:
            if i['r0']['value'] != "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" and \
                    i['r0']['value'] != "http://www.w3.org/2000/01/rdf-schema#label":
                rnt.append((src, i['r0']['value'], i['t0']['value']))

        return rnt


    def deduce_subgraph_by_path_one(self, src: str, rels: List[str]):

        query = f"""
                SELECT DISTINCT ?e1 WHERE {{
                    <{src}> <{rels[0]}> ?e1 . 
                }} LIMIT 2000 
                """

        self.sparql.setQuery(query)

        try:
            results = self.sparql.query().convert()
        except urllib.error.URLError:
            print(query)
            exit(0)

        nodes = [i['e1']['value']
                 for i in results['results']['bindings']] + [src]
        triples = [(src, rels[0], i['e1']['value'])
                   for i in results['results']['bindings']]
        nodes = list(set(nodes))
        triples = list(set(triples))

        return nodes, triples


    def deduce_subgraph_by_path_two(self, src: str, rels: List[str]):

        query = f"""
                SELECT DISTINCT ?e1 ?e2 WHERE {{
                    <{src}> <{rels[0]}> ?e1 . 
                    ?e1 <{rels[1]}> ?e2 .
                }} LIMIT 2000 
                """

        self.sparql.setQuery(query)

        try:
            results = self.sparql.query().convert()
        except urllib.error.URLError:
            print(query)
            exit(0)

        nodes = [i['e1']['value'] for i in results['results']['bindings']] + \
                [i['e2']['value']
                 for i in results['results']['bindings']] + [src]
        triples = [(src, rels[0], i['e1']['value']) for i in results['results']['bindings']] + \
                  [(i['e1']['value'], rels[1], i['e2']['value'])
                   for i in results['results']['bindings']]
        nodes = list(set(nodes))
        triples = list(set(triples))

        return nodes, triples

    def deduce_subgraph_by_path(self, src: str, path: List[str]) -> Tuple[
        List[str], List[Tuple[str, str, str]]]:

        if len(path) > 2:
            print(path)
        assert len(path) <= 2
        if len(path) == 0:
            return ([src], [])
        elif len(path) == 1:
            return self.deduce_subgraph_by_path_one(src, path)
        elif len(path) == 2:
            return self.deduce_subgraph_by_path_two(src, path)





if __name__ == '__main__':
    endpoint = "https://skynet.coypu.org/coypu-internal/"
    credentials = yaml.safe_load(open("./credentials.yml", "r"))
    coypukg = KnowledgeGraphCoyPu(endpoint)
    coypukg.setCredentials(credentials["user"], credentials["pass"])

    # Example 1
    entity = "https://data.coypu.org/event/emdat/2017-0548-OMN"
    relations = coypukg.get_relations(entity)
    print(relations)

    # Example 2
    src = "https://data.coypu.org/event/emdat/2017-0548-OMN"
    relation = "https://schema.coypu.org/global#hasAffectedRegion"
    tails = coypukg.get_tails(src, relation)
    print(tails)

    # Example 3
    src = "https://data.coypu.org/event/emdat/2017-0548-OMN"
    one_hop_paths = coypukg.get_one_hop_paths(src)
    print(one_hop_paths)

    # Example 4
    src = "https://data.coypu.org/event/emdat/2017-0548-OMN"
    rels = ["https://schema.coypu.org/global#hasCountryLocation", "https://schema.coypu.org/global#hasContinent"]
    subgraph = coypukg.deduce_subgraph_by_path(src, rels)

    print(subgraph)

    print("")
