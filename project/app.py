import os
import math
from typing import List, Optional
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, confloat, conint
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from math import radians, cos, sin, asin, sqrt
import h3


import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("h3-api")

def densify_polyline_m(poly: np.ndarray, max_step_m: float = 5.0) -> np.ndarray:
    if len(poly) < 2:
        return poly
    out = [poly[0]]
    for i in range(len(poly)-1):
        a = poly[i]; b = poly[i+1]
        seg_len = haversine_m(tuple(a), tuple(b))
        if seg_len <= max_step_m:
            out.append(b); continue
        n = int(np.ceil(seg_len / max_step_m))
        for t in np.linspace(0, 1, n+1)[1:]:
            out.append(a*(1-t) + b*t)
    return np.vstack(out)

def haversine_m(p1, p2):
    # p = (lon,lat)
    lon1, lat1 = map(radians, p1)
    lon2, lat2 = map(radians, p2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 6371000.0 * 2 * asin(sqrt(a))  # meters

def closest_point_on_segment(a, b, p):
    # all as (lon,lat); use local equirectangular for small spans
    # project to x/y meters around p's latitude
    R = 6371000.0
    lat0 = radians(p[1])
    def to_xy(q):
        lon, lat = map(radians, q)
        x = R * (lon - radians(p[0])) * cos(lat0)
        y = R * (lat - radians(p[1]))
        return np.array([x, y])
    A, B, P = map(to_xy, (a, b, p))
    AB = B - A
    if np.allclose(AB, 0):
        t = 0.0
        Q = A
    else:
        t = np.clip(np.dot(P - A, AB) / np.dot(AB, AB), 0.0, 1.0)
        Q = A + t * AB
    # back to lon/lat
    xq, yq = Q
    lon = p[0] + (xq / (R * cos(lat0)))
    lat = p[1] + (yq / R)
    return (lon, lat)

def nearest_point_on_polyline(poly, p):
    # poly: Nx2 (lon,lat), p: (lon,lat)
    best_q, best_d = None, float("inf")
    for i in range(len(poly)-1):
        q = closest_point_on_segment(tuple(poly[i]), tuple(poly[i+1]), p)
        d = haversine_m(q, p)
        if d < best_d:
            best_q, best_d = q, d
    return best_q, best_d

def cap_deviation(smoothed: np.ndarray, raw: np.ndarray, max_dev_m: float) -> np.ndarray:
    if max_dev_m is None or max_dev_m <= 0:
        return smoothed
    out = smoothed.copy()
    # lock endpoints
    out[0]  = raw[0]
    out[-1] = raw[-1]
    for i in range(1, len(out)-1):
        p = tuple(out[i])
        q, d = nearest_point_on_polyline(raw, p)
        if d > max_dev_m:
            # move p toward q so the distance equals max_dev_m
            # linear blend in lon/lat (ok for small steps)
            alpha = (d - max_dev_m) / d
            out[i] = ( (1 - alpha)*p[0] + alpha*q[0],
                       (1 - alpha)*p[1] + alpha*q[1] )
    return out

def equirect_m2(p, q, lat0_rad=None):
    # fast approx squared distance in meters^2 (good for small deltas)
    R = 6371000.0
    lon1, lat1 = map(radians, p)
    lon2, lat2 = map(radians, q)
    if lat0_rad is None:
        lat0_rad = 0.5*(lat1+lat2)
    dx = (lon2 - lon1) * cos(lat0_rad) * R
    dy = (lat2 - lat1) * R
    return dx*dx + dy*dy

def closest_point_on_segment_fast(a, b, p):
    # all (lon,lat); local equirectangular around p
    R = 6371000.0
    lat0 = radians(p[1])
    def to_xy(q):
        lon, lat = map(radians, q)
        return np.array([R*(lon - radians(p[0]))*cos(lat0), R*(lat - radians(p[1]))])
    A, B, P = map(to_xy, (a, b, p))
    AB = B - A
    if np.allclose(AB, 0):
        Q = A
    else:
        t = np.clip(np.dot(P - A, AB) / np.dot(AB, AB), 0.0, 1.0)
        Q = A + t*AB
    # back to lon/lat
    xq, yq = Q
    lon = p[0] + (xq / (R * cos(lat0)))
    lat = p[1] + (yq / R)
    return (lon, lat)

def cap_deviation_local(smoothed: np.ndarray, raw: np.ndarray, max_dev_m: float, window: int = 25) -> np.ndarray:
    if not (max_dev_m and max_dev_m > 0) or len(raw) < 2 or len(smoothed) < 3:
        return smoothed
    out = smoothed.copy()
    out[0]  = raw[0]
    out[-1] = raw[-1]
    j = 0  # segment cursor on raw polyline
    lat0 = radians(raw[0][1])
    max_dev2 = max_dev_m * max_dev_m
    for i in range(1, len(out)-1):
        p = tuple(out[i])
        best_q, best_d2, best_seg = None, float("inf"), j
        lo = max(0, j - window)
        hi = min(len(raw) - 2, j + window)
        for k in range(lo, hi + 1):
            q = closest_point_on_segment_fast(tuple(raw[k]), tuple(raw[k+1]), p)
            d2 = equirect_m2(p, q, lat0)
            if d2 < best_d2:
                best_d2, best_q, best_seg = d2, q, k
                if best_d2 <= max_dev2:
                    break
        j = best_seg  # advance cursor along the path
        if best_d2 > max_dev2:
            d = math.sqrt(best_d2)
            alpha = (d - max_dev_m) / d
            out[i] = ((1 - alpha)*p[0] + alpha*best_q[0],
                      (1 - alpha)*p[1] + alpha*best_q[1])
    return out


RES = 11

def _h3f():
    f = {}
    if hasattr(h3, "geo_to_h3"):  # v3
        f["latlon_to_cell"] = lambda lat, lon, res: h3.geo_to_h3(lat, lon, res)
        f["cell_to_latlon"] = lambda cell: h3.h3_to_geo(cell)  # (lat,lon)
    else:  # v4
        f["latlon_to_cell"] = lambda lat, lon, res: h3.latlng_to_cell(lat, lon, res)
        f["cell_to_latlon"] = lambda cell: h3.cell_to_latlng(cell)  # (lat,lon)
    return f

H3F = _h3f()

# --- Smoothing (SciPy optional) ---
try:
    from scipy.interpolate import splprep, splev
    HAVE_SCIPY = True
except Exception:
    HAVE_SCIPY = False

def chaikin(points: np.ndarray, iters: int = 2) -> np.ndarray:
    pts = points.copy()
    for _ in range(iters):
        new_pts = [pts[0]]
        for i in range(len(pts)-1):
            p, q = pts[i], pts[i+1]
            Q = 0.75*p + 0.25*q
            R = 0.25*p + 0.75*q
            new_pts.extend([Q, R])
        new_pts.append(pts[-1])
        pts = np.vstack(new_pts)
    return pts

def spline(points: np.ndarray, s: float = 0.0, num: int = 400) -> np.ndarray:
    if not HAVE_SCIPY or len(points) < 3:
        return points
    x, y = points[:,0], points[:,1]
    d = np.r_[0, np.cumsum(np.hypot(np.diff(x), np.diff(y)))]
    if d[-1] == 0:
        return points
    u = d / d[-1]
    try:
        tck, _ = splprep([x, y], u=u, s=s, k=min(2, len(points)-1))  # k=2, s≈0
        uu = np.linspace(0, 1, num)
        xs, ys = splev(uu, tck)
        return np.column_stack([xs, ys])
    except Exception:
        return points


# ---------- API models ----------
class Point(BaseModel):
    lat: confloat(ge=-90, le=90)
    lon: confloat(ge=-180, le=180)

class RouteReq(BaseModel):
    start: Point
    end: Point
    smooth: bool = True
    method: str = Field("spline", description="spline|chaikin")
    s: confloat(ge=0.0, le=10.0) = 0.0      # hug the path
    npts: conint(ge=50, le=2000) = 400
    iters: conint(ge=1, le=6) = 1
    max_dev_m: Optional[confloat(ge=0.0)] = 5.0   # really tight leash


app = FastAPI(title="H3+Neo4j Route API", version="1.0")

# ---------- Neo4j driver ----------
NEO4J_URI  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")   # inside compose use bolt://neo4j:7687
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "password")
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def _wait_for_neo4j(timeout=120):
    import time
    start = time.time()
    while True:
        try:
            with driver.session() as s:
                s.run("RETURN 1").single()
            return
        except ServiceUnavailable:
            if time.time() - start > timeout:
                raise
            time.sleep(2)

@app.on_event("startup")
def _startup():
    _wait_for_neo4j()

@app.get("/healthz")
def healthz():
    try:
        with driver.session() as s:
            s.run("RETURN 1").single()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------- core helpers ----------
def h3_centroid_line(cells: List[str]) -> np.ndarray:
    # (lon, lat) for GeoJSON
    return np.array([(H3F["cell_to_latlon"](c)[1], H3F["cell_to_latlon"](c)[0]) for c in cells], dtype=float)

def geojson_line(coords: np.ndarray, props: dict):
    return {
        "type": "Feature",
        "properties": props,
        "geometry": {"type": "LineString", "coordinates": coords.tolist()},
    }

def shortest_path_hexes(from_hex: str, to_hex: str) -> List[str]:
    """
    Use GDS Dijkstra on pre-projected 'myGraph'.
    Same shape as your previously working query:
      - nodeLabels:['H3']
      - relationshipTypes:['CAN_PASS']
      - sourceNode: source  (matched by hex_name)
      - targetNode: target
    Returns the ordered list of H3 hex_name along the path (or [] if none).
    """
    cypher = """
    MATCH (source:H3 {hex_name: $from_hex}), (target:H3 {hex_name: $to_hex})
    CALL gds.shortestPath.dijkstra.stream('myGraph', {
      nodeLabels: ['H3'],
      relationshipTypes: ['CAN_PASS'],
      relationshipWeightProperty: 'cost',
      sourceNode: source,
      targetNode: target
    })
    YIELD path, nodeIds
    RETURN [nodeId IN nodeIds | gds.util.asNode(nodeId).hex_name] AS nodeNames
    """
    with driver.session() as sess:
        rec = sess.run(cypher, parameters={"from_hex": from_hex, "to_hex": to_hex}).single()
        if not rec or rec.get("nodeNames") is None:
            return []
        return rec["nodeNames"]


def latlon_to_hex(lat: float, lon: float) -> str:
    return H3F["latlon_to_cell"](lat, lon, RES)

# ---------- endpoint ----------
@app.post("/route")
def route(req: RouteReq):
    # 1) start/end hex at requested res
    from_hex = latlon_to_hex(req.start.lat, req.start.lon)
    to_hex   = latlon_to_hex(req.end.lat,   req.end.lon)

        # log them
    logger.info(f"/route start_hex={from_hex} end_hex={to_hex} "
                f"start=({req.start.lat:.6f},{req.start.lon:.6f}) "
                f"end=({req.end.lat:.6f},{req.end.lon:.6f}) res={RES}")

    # 2) ask Neo4j GDS for path over CAN_PASS
    hexes = shortest_path_hexes(from_hex, to_hex)
    if not hexes:
        raise HTTPException(404, detail="No path found in graph for given start/end at this resolution.")

    # 3) centroids -> LineStrings (raw + smoothed)
    raw = h3_centroid_line(hexes)

    if req.smooth:
        if req.method == "spline":
            smooth = spline(raw, s=req.s, num=req.npts)
        else:
            smooth = chaikin(raw, iters=req.iters)
        smooth = cap_deviation_local(smooth, raw, req.max_dev_m, window=25)
    else:
        smooth = raw


    fc = {
        "type": "FeatureCollection",
        "features": [
            geojson_line(raw, {"kind": "raw_centroids", "res": RES, "n": len(hexes)}),
            geojson_line(smooth, {"kind": "smoothed", "method": req.method})
        ]
    }
    return fc
