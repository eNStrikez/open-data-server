from flask import Flask, jsonify, request
from flask_cors import CORS
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import SDO, XSD
from rdflib.plugins.sparql import prepareQuery
import pandas as pd

app = Flask(__name__)
CORS(app)

ttl_root = "https://raw.githubusercontent.com/eNStrikez/open-data-1/main/rdf/"
ttl_schema = "https://raw.githubusercontent.com/eNStrikez/open-data-1/main/rdf/schema.ttl"
schema = Namespace('https://enstrikez.github.io/open-data-1/schema/')
graphs = {}
files = ["sample_size", "response_rates", "trading_status", "government_schemes_1", "government_schemes_2", "government_schemes_3"]

survey_query = prepareQuery(
    """SELECT DISTINCT ?aname ?bname ?c
       WHERE {
          ?a ?dim1 ?aid .
          ?a ?dim2 ?bid .
          ?aid sdo:name ?aname .
          ?bid sdo:name ?bname .
          ?a sdo:value ?c .
       }""", initNs={'sdo': SDO})

aggregated_query = prepareQuery(
    """SELECT ?aname (GROUP_CONCAT(DISTINCT CONCAT(?bname, CONCAT("|", STR(?c))); SEPARATOR="~") AS ?var)
       WHERE {
          ?a ?dim1 ?aid .
          ?a ?dim2 ?bid .
          ?aid sdo:name ?aname .
          ?bid sdo:name ?bname .
          ?a sdo:value ?c .
          ?a gov:valueType ?valType .
       } GROUP BY ?aname""", initNs={'sdo': SDO, 'gov': schema})

meta_query = prepareQuery(
    """SELECT DISTINCT ?aname ?bname ?cname
       WHERE {
          ?a ?dim1 ?aid .
          ?a ?dim2 ?bid .
          ?a gov:valueType ?bname .
          ?a sdo:name ?aname .
          ?a sdo:description ?cname
       }""", initNs={'sdo': SDO, 'gov': schema})


@app.before_first_request
def init():
    print("Initialising server")
    for file in files:
        g = Graph()
        g.parse(ttl_root + file + '.ttl', format="turtle")
        graphs.update({file: g})


@app.route('/meta', methods=['GET'])
def get_meta():
    dim1 = request.args.get('x')
    dim2 = request.args.get('y')
    if dim1 == 'Industry':
        dim1 = schema.hasIndustry
    elif dim1 == 'Country':
        dim1 = schema.hasCountry
    elif dim1 == 'Workforce Size':
        dim1 = schema.hasWorkforce
    elif dim1 == 'Trading Status':
        dim1 = schema.hasStatus
    else:
        dim1 = schema.hasScheme

    if dim2 == 'Industry':
        dim2 = schema.hasIndustry
    elif dim2 == 'Country':
        dim2 = schema.hasCountry
    elif dim2 == 'Workforce Size':
        dim2 = schema.hasWorkforce
    elif dim2 == 'Trading Status':
        dim2 = schema.hasStatus
    else:
        dim2 = schema.hasScheme

    rows = graphs[request.args.get('name')].query(meta_query, initBindings={'dim1': dim1, 'dim2': dim2})
    query = []
    for row in rows:
        query.append([row[0], row[1], row[2]])
    return pd.DataFrame(query, columns=['Name', 'Type', 'Desc']).to_json(orient="records")


@app.route('/series', methods=['GET'])
def get_series():
    dim1 = request.args.get('x')
    dim2 = request.args.get('y')
    if dim1 == 'Industry':
        dim1 = schema.hasIndustry
    elif dim1 == 'Country':
        dim1 = schema.hasCountry
    elif dim1 == 'Workforce Size':
        dim1 = schema.hasWorkforce
    elif dim1 == 'Trading Status':
        dim1 = schema.hasStatus
    else:
        dim1 = schema.hasScheme

    if dim2 == 'Industry':
        dim2 = schema.hasIndustry
    elif dim2 == 'Country':
        dim2 = schema.hasCountry
    elif dim2 == 'Workforce Size':
        dim2 = schema.hasWorkforce
    elif dim2 == 'Trading Status':
        dim2 = schema.hasStatus
    else:
        dim2 = schema.hasScheme

    rows = graphs[request.args.get('name')].query(aggregated_query, initBindings={'dim1': dim1, 'dim2': dim2, 'valType': Literal(request.args.get('val'), datatype=XSD.string)})
    series = []
    for row in rows:
        q = row[1]
        values = q.split("~")
        kvs = {}
        for v in values:
            kv = v.split("|")
            kvs.update({kv[0]: kv[1]})
        series.append([row[0], kvs])
    return pd.DataFrame(series, columns=['Name', 'Values']).to_json(orient="records")

if __name__ == '__main__':
    app.run()
