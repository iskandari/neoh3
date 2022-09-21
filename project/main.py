import h3
from neo4j import GraphDatabase, Session
from neo4j.exceptions import ConstraintError
import time
import logging

logging.basicConfig(level=logging.INFO)

uri = "neo4j://neo4j:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

  # close the driver object

def prepare_hexes():
    logging.info("Preparing hexes")

    with driver.session() as sess:
        result = sess.run(
            """
                MATCH (n:H3) RETURN count(n) AS c
        """
        )
        req = int(result.single()["c"])
        logging.debug(f"Number of H3 nodes:{req}")

    if req == 31488:  # Then H3 nodes are already created
        logging.info('H3 nodes have been created already.')
        return

    logging.info('H3 nodes are being created.')

    with open("data/distinct_h3_hexes.csv") as distinct_h3_hexes:
        distinct_hexes_lines = distinct_h3_hexes.readlines()
        sep = None
        for x in [",", "\t", ";", "|"]:
            if x in distinct_hexes_lines[0]:
                sep = x
        if sep is not None:
            id = [line.split(sep=sep)[0].replace("\n", "")
                  for line in distinct_hexes_lines]
            hex = [line.split(sep=sep)[1].replace("\n", "")
                   for line in distinct_hexes_lines]
        else:
            logging.error("Separator not found - 1")

    distinct_hexes = [
        {'id': int(id[i]), 'hex': hex[i]} for i in range(len(id))
    ]  # List of dict [{'id': id, 'hex': hex}, {...}, ...]
    with driver.session() as sess:

        # Query to create all nodes
        node_create_query = """
                UNWIND $distinct_h3 as h3
                CALL apoc.create.node(["H3"], {id: h3['id'], hex_name:h3['hex']})
                YIELD node
                RETURN node;"""
        sess.run(node_create_query, distinct_h3=distinct_hexes)

        # This query creates a constraint on distinct h3 IDs.
        sess.run(
            """
            CREATE CONSTRAINT H3_id IF NOT EXISTS on (n:H3) ASSERT n.id IS UNIQUE
            """
        )

        # Create index on the hex name for faster search
        sess.run(
            """
            CREATE INDEX H3_hex_hame IF NOT EXISTS
            FOR (h:H3)
            ON (h.hex_name)
            """
        )

    logging.info('All nodes created')
    del distinct_hexes
    with open("data/hex_maze.csv") as hex_maze:
        hex_maze_lines = hex_maze.readlines()
        sep = None
        for x in [",", "\t", ";", "|"]:
            if x in hex_maze_lines[0]:
                sep = x
        if sep is not None:
            srcs = [int(line.split(sep=",")[0].replace("\n", ""))
                    for line in hex_maze_lines]
            dsts = [int(line.split(sep=",")[1].replace("\n", ""))
                    for line in hex_maze_lines]
            cost = [int(line.split(sep=",")[2].replace("\n", ""))
                    for line in hex_maze_lines]
        else:
            logging.error("Separator not found - 2")

    h3_maze = [
        {'from': srcs[i], 'to': dsts[i], 'cost': cost[i]} for i in range(len(srcs))
    ]  # List of dictionaries: [{"from": source, "to": destination, "cost": cost}, {...}, ...]
    with driver.session() as sess:
        start = time.monotonic()
        logging.info("Relations are being created")

        # Query to create relations between nodes
        query = """
                UNWIND $h3_maze as h3_path
                MATCH (src:H3 {id:h3_path["from"]})
                MATCH (dst:H3 {id:h3_path["to"]})
                CALL apoc.create.relationship(src, "CAN_PASS", {cost: h3_path["cost"]}, dst)
                YIELD rel
                RETURN count(*)
        """

        rslt = sess.run(query, h3_maze=h3_maze)

        logging.info(
            f'{rslt.single()["count(*)"]} relations were created in {round((time.monotonic() - start), 2)} seconds')
   
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
            logging.info('Graph created')
        except Exception as e:
            logging.info(e)


driver.close()

if __name__ == "__main__":
    prepare_hexes()



