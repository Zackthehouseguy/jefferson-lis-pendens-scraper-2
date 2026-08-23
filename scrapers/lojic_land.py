"""Resilient LOJIC parcel/zoning/land-use adapter for TheReaper land lane."""
from __future__ import annotations
import json, time
import requests

PVA_QUERY='https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataPVA/MapServer/1/query'
ZONING_QUERY='https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/15/query'
LANDUSE_QUERY='https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/6/query'


def clean(v): return ' '.join(str(v or '').split())

def _request(method,url,*,params=None,data=None,attempts=4,timeout=25):
    last=None
    for i in range(attempts):
        try:
            r=requests.request(method,url,params=params,data=data,timeout=timeout,headers={'User-Agent':'TheREaperPublicRecords/1.0'})
            r.raise_for_status(); p=r.json()
            if p.get('error'): raise RuntimeError(p['error'])
            return p, {'attempts':i+1,'recovered':i>0,'errors':[] if i==0 else [str(last)]}
        except Exception as exc:
            last=exc
            if i+1<attempts: time.sleep(min(4.0,0.75*(2**i)))
    raise last

def _representative_point(feature:dict):
    c=feature.get('centroid')
    if c and c.get('x') is not None and c.get('y') is not None: return {'x':c['x'],'y':c['y']}
    geom=feature.get('geometry') or {}; rings=geom.get('rings') or []
    pts=[p for ring in rings for p in ring if isinstance(p,list) and len(p)>=2]
    if not pts: return None
    # bbox center keeps request tiny; spatial query is only an enrichment estimate.
    xs=[p[0] for p in pts]; ys=[p[1] for p in pts]
    return {'x':(min(xs)+max(xs))/2.0,'y':(min(ys)+max(ys))/2.0}

def _point_layer(url,point,fields):
    if not point: return None, {'attempts':0,'recovered':False,'errors':['no_representative_point']}
    params={'where':'1=1','geometry':f"{point['x']},{point['y']}",'geometryType':'esriGeometryPoint','inSR':'2246',
            'spatialRel':'esriSpatialRelIntersects','outFields':fields,'returnGeometry':'false','f':'json'}
    p,meta=_request('GET',url,params=params)
    feats=p.get('features') or []
    return ((feats[0].get('attributes') or {}) if feats else None),meta

def enrich_parcel(parcel_id:str)->tuple[dict,list[dict]]:
    failures=[]; result={'parcel_type':None,'lot_sqft':None,'lot_acres':None,'pin':None,'zoning_code':None,
      'zoning_name':None,'zoning_type':None,'landuse_name':None,'lojic_parcel_verified':False,'lojic_recovery':{}}
    try:
        escaped=parcel_id.replace("'","''")
        params={'where':f"PARCELID='{escaped}'",'outFields':'PARCELID,PARCEL_TYPE,PIN,SHAPE.AREA','returnGeometry':'true',
                'returnCentroid':'true','outSR':'2246','f':'json'}
        p,meta=_request('GET',PVA_QUERY,params=params); result['lojic_recovery']['parcel']=meta
        feats=p.get('features') or []
        if not feats: return result,[{'source':'lojic_parcel','reason':'parcel_not_found'}]
        feat=feats[0]; a=feat.get('attributes') or {}; sqft=a.get('SHAPE.AREA')
        try: sqft=float(sqft) if sqft is not None else None
        except Exception: sqft=None
        result.update({'parcel_type':a.get('PARCEL_TYPE'),'lot_sqft':round(sqft,1) if sqft is not None else None,
          'lot_acres':round(sqft/43560.0,4) if sqft is not None else None,'pin':a.get('PIN'),'lojic_parcel_verified':True})
        point=_representative_point(feat)
        try:
            z,zm=_point_layer(ZONING_QUERY,point,'ZONING_CODE,ZONING_NAME,ZONING_TYPE'); result['lojic_recovery']['zoning']=zm
            if z: result.update({'zoning_code':clean(z.get('ZONING_CODE')) or None,'zoning_name':clean(z.get('ZONING_NAME')) or None,'zoning_type':clean(z.get('ZONING_TYPE')) or None})
            else: failures.append({'source':'lojic_zoning','reason':'no_intersection'})
        except Exception as exc: failures.append({'source':'lojic_zoning','reason':f'{type(exc).__name__}:{exc}'})
        try:
            l,lm=_point_layer(LANDUSE_QUERY,point,'LANDUSE_NAME'); result['lojic_recovery']['landuse']=lm
            if l: result['landuse_name']=clean(l.get('LANDUSE_NAME')) or None
            else: failures.append({'source':'lojic_landuse','reason':'no_intersection'})
        except Exception as exc: failures.append({'source':'lojic_landuse','reason':f'{type(exc).__name__}:{exc}'})
    except Exception as exc:
        failures.append({'source':'lojic_parcel','reason':f'{type(exc).__name__}:{exc}'})
    return result,failures
