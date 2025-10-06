import csv
import logging
import time
from itertools import islice
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- CONFIG ---
# If running this script on the HOST, use localhost; inside docker network, use 'neo4j'
logging.basicConfig(level=logging.INFO)

NEO4J_URI  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "password")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

DISTINCT_PATH = "data/distinct_h3_hexes.csv"      # no header, rows: idx,h3
MAZE_PATH     = "data/hex_maze.csv"               # no header, rows: src_idx,dst_idx,cost
BATCH_NODES   = 50_000
BATCH_EDGES   = 50_000

def wait_for_neo4j(timeout=120):
    start = time.time()
    while True:
        try:
            with driver.session() as s:
                s.run("RETURN 1").single()
            logging.info("Neo4j is reachable.")
            return
        except ServiceUnavailable:
            if time.time() - start > timeout:
                raise
            logging.info("Waiting for Neo4j to come up...")
            time.sleep(2)

def chunked(reader, size):
    while True:
        block = list(islice(reader, size))
        if not block:
            break
        yield block

def prepare_hexes():
    with driver.session() as sess:
        # 0) Constraints & indexes FIRST
        logging.info("Creating constraints / indexes (if not exists)")
        sess.run("CREATE CONSTRAINT h3_id IF NOT EXISTS FOR (h:H3) REQUIRE h.id IS UNIQUE")
        sess.run("""
            CREATE INDEX h3_hex_name IF NOT EXISTS
            FOR (h:H3) ON (h.hex_name)
        """)

        # 1) Nodes
        start = time.monotonic()
        current_nodes = sess.run("MATCH (n:H3) RETURN count(n) AS c").single()["c"]
        logging.info(f"Existing H3 nodes: {current_nodes}")

        if current_nodes == 0:
            logging.info("Loading nodes from distinct_h3_hexes.csv ...")
            with open(DISTINCT_PATH, newline="") as f:
                rdr = csv.reader(f)
                total = 0
                for block in chunked(rdr, BATCH_NODES):
                    # block entries: [idx, h3]
                    records = [{"id": int(r[0]), "hex": r[1]} for r in block if len(r) >= 2]
                    sess.run(
                        """
                        UNWIND $batch AS row
                        MERGE (h:H3 {id: row.id})
                        ON CREATE SET h.hex_name = row.hex
                        """,
                        batch=records
                    )
                    total += len(records)
                    logging.info(f"Nodes committed: {total:,}")
            logging.info(f"Nodes loaded in {time.monotonic() - start:.2f}s")
        else:
            logging.info("Nodes already present. Skipping node load.")

        # 2) Relationships (chunked)
        logging.info("Loading relationships (CAN_PASS) from hex_maze.csv ...")
        start = time.monotonic()
        inserted = 0
        with open(MAZE_PATH, newline="") as f:
            rdr = csv.reader(f)
            for block in chunked(rdr, BATCH_EDGES):
                rows = []
                for r in block:
                    if len(r) < 3:
                        continue
                    try:
                        rows.append({"src": int(r[0]), "dst": int(r[1]), "cost": int(r[2])})
                    except ValueError:
                        continue
                if not rows:
                    continue
                # MERGE nodes on-the-fly to guarantee endpoints exist (id is unique)
                sess.run(
                    """
                    UNWIND $batch AS row
                    MERGE (a:H3 {id: row.src})
                    MERGE (b:H3 {id: row.dst})
                    MERGE (a)-[:CAN_PASS {cost: row.cost}]->(b)
                    """,
                    batch=rows
                )
                inserted += len(rows)
                logging.info(f"Relationships committed: {inserted:,}")

        rel_count = sess.run("MATCH ()-[r:CAN_PASS]->() RETURN count(r) AS c").single()["c"]
        logging.info(f"Total relationships in DB: {rel_count:,} (loaded {inserted:,} this run) in {time.monotonic() - start:.2f}s")

        # 3) (Optional) GDS projection
        try:
            sess.run("CALL gds.graph.drop('myGraph', false) YIELD graphName")
        except Exception:
            pass
        try:
            sess.run("""
                CALL gds.graph.project(
                  'myGraph',
                  'H3',
                  { CAN_PASS: { type: 'CAN_PASS', properties: 'cost' } }
                )
            """)
            logging.info("GDS graph 'myGraph' projected.")
        except Exception as e:
            logging.info(f"GDS projection skipped/failed: {e}")

def main():
    wait_for_neo4j()
    prepare_hexes()

if __name__ == "__main__":
    main()
    driver.close()
