from neo4j import __version__ as neo4j_version
import streamlit as st
from streamlit_folium import st_folium
print(neo4j_version)
import pandas as pd
import h3
from shapely.geometry import Polygon, LineString
import json
from geojson import Feature, Point, FeatureCollection
from neo4j import GraphDatabase
import folium
import geopandas as gpd
import logging


st.title("Shortest maritime paths H3")


class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__pwd)
            )
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = (
                self.__driver.session(database=db)
                if db is not None
                else self.__driver.session()
            )
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


def hexagons_dataframe_to_geojson(df_hex, file_output=None, column_name="value"):
    """
    Produce the GeoJSON for a dataframe, constructing the geometry from the "hex_id" column
    and with a property matching the one in column_name
    """
    foo = LineString([(-90, 179), (90, 179)])
    bar = LineString([(-90, -179), (90, -179)])

    list_features = []

    for i, row in df_hex.iterrows():

        h3.h3_to_geo_boundary(h=row["hex_id"])

        try:
            geometry_for_row = {
                "type": "Polygon",
                "coordinates": [h3.h3_to_geo_boundary(h=row["hex_id"], geo_json=True)],
            }
            poly = Polygon(h3.h3_to_geo_boundary(row["hex_id"]))
            if not foo.intersects(poly) and not bar.intersects(poly):
                feature = Feature(
                    geometry=geometry_for_row,
                    id=row["hex_id"],
                    properties={column_name: row[column_name]},
                )
                list_features.append(feature)
            else:
                print("intersected prime meridian!")
        except:
            print("An exception occurred for hex " + row["hex_id"])

    feat_collection = FeatureCollection(list_features)
    geojson_result = json.dumps(feat_collection)
    return geojson_result


uri = "neo4j://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

with driver.session() as sess:
    
    query_string = f"""
    CALL gds.graph.project(
    'myGraph',
    'H3',
    'CAN_PASS',
    {{
        relationshipProperties: 'cost'
    }}
    )
    """

    try:
        sess.run(query_string)
        res = sess.run(query_string)        
    
        if 'already exists' in res:
            logging.info('myGraph exists')
        else:
            'graph created !'
        
    except Exception as e:
        if e.__class__.__name__ == 'ClientError':
            logging.info=("Graph already exists, skipping creation.")
        


def shortest_path(from_hex, to_hex):

    with driver.session() as sess:
        query_string = f"""

        MATCH (source:H3 {{hex_name: '{from_hex}'}}), (target:H3 {{hex_name: '{to_hex}'}})
        CALL gds.shortestPath.dijkstra.stream('myGraph', {{
        nodeLabels:['H3'],
        relationshipTypes:['CAN_PASS'],
        relationshipWeightProperty: 'cost',
        sourceNode:source,
        targetNode:target}})
        YIELD path, nodeIds
        RETURN
        [nodeId IN nodeIds | gds.util.asNode(nodeId).hex_name] AS nodeNames,
        nodes(path) as path;
        
        """
        res = sess.run(query_string)
        res = [record for record in res]
        return res 


#mylist = shortest_path("833849fffffffff", "83318dfffffffff")

form = st.form(key='my-form')
from_hex = form.text_input('Enter origin hex id')
to_hex = form.text_input('Enter destination hex id')
submit = form.form_submit_button('Submit')

if submit:
    with st.spinner('Calculating path'):

        mylist = shortest_path(f'{from_hex}', f'{to_hex}')

        # #df = pd.DataFrame(res)
        # # df.to_csv('shortest_path_dijkstra.csv', index=False)

        res = pd.DataFrame(list(mylist[0][0]), columns=["hex_id"])

        st.write(res)

        test = hexagons_dataframe_to_geojson(res, file_output=None, column_name="hex_id")
        m = folium.Map(location=[40.70, -73.94], zoom_start=2, tiles="CartoDB positron")
        geo_j = folium.GeoJson(data=test, style_function=lambda x: {"fillColor": "orange"})
        geo_j.add_to(m)

        st_data = st_folium(m, width=725)

        driver.close()