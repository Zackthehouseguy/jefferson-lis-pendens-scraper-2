"""Resilient LOJIC parcel/zoning/land-use adapter for TheReaper land lane."""
from __future__ import annotations
import time
import requests

PVA_QUERY='https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataPVA/MapServer/1/query'
ZONING_QUERY='https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/15/query'
LANDUSE_QUERY='https://gis.lojic.org/maps/rest/services/LojicSolutions/OpenDataDevelopment/MapServer/6/query'
_SESSION=requests.Session()
_CACHE={}
_LAST_REQUEST_AT=0.0
MIN_REQUEST_GAP=0.45


def clean(v): return ' '.join(str(v or '').split())

def _pace():
    global _LAST_REQUEST_AT
    now=time.monotonic(); wait=MIN_REQUEST_GAP-(now-_LAST_REQUEST_AT)
    if wait>0: time.sleep(wait)
    _LAST_REQUEST_AT=time.monotonic()

def _request(method,url,*,params=None,data=None,attempts=6,timeout=30):
    last=None; errors=[]
    for i in range(attempts):
        try:
            _pace()
            r=_SESSION.request(method,url,params=params,data=data,timeout=timeout,headers={'User-Agent':'TheReaperPublicRecords/1.0','Accept':'application/json,text/plain,*/*'})
            if r.status_code in (429,500,502,503,504):
                raise RuntimeError(f'retryable_http_{r.status_code}')
            r.raise_for_status()
            body=(r.text or '').strip()
            if not body: raise RuntimeError('empty_response_body')
            ctype=(r.headers.get('content-type') or '').lower()
            if 'html' in ctype or body.startswith('<!DOCTYPE') or body.startswith('<html'):
                raise RuntimeError('non_json_html_response')
            try: p=r.json()
            except Exception as exc: raise RuntimeError(f'json_decode_failed:{type(exc).__name__}') from exc
            if not isinstance(p,dict): raise RuntimeError('unexpected_json_shape')
            if p.get('error'): raise RuntimeError(f"arcgis_error:{p['error']}")
            return p, {'attempts':i+1,'recovered':i>0,'errors':errors}
        except Exception as exc:
            last=exc; errors.append(f'{type(exc).__name__}:{exc}')
            if i+1<attempts: time.sleep(min(8.0,0.8*(2**i)))
    raise last

def _representative_point(feature:dict):
    c=feature.get('centroid')
    if c and c.get('x') is not None and c.get('y') is not None:return {'x':c['x'],'y':c['y']}
    geom=feature.get('geometry') or {};rings=geom.get('rings') or []
    pts=[p for ring in rings for p in ring if isinstance(p,list) and len(p)>=2]
    if not pts:return None
    xs=[p[0] for p in pts];ys=[p[1] for p in pts]
    return {'x':(min(xs)+max(xs))/2.0,'y':(min(ys)+max(ys))/2.0}

def _point_layer(url,point,fields):
    if not point:return None,{'attempts':0,'recovered':False,'errors':['no_representative_point']}
    params={'where':'1=1','geometry':f"{point['x']},{point['y']}",'geometryType':'esriGeometryPoint','inSR':'2246','spatialRel':'esriSpatialRelIntersects','outFields':fields,'returnGeometry':'false','f':'json'}
    p,meta=_request('GET',url,params=params)
    feats=p.get('features') or []
    return ((feats[0].get('attributes') or {}) if feats else None),meta

def enrich_parcel(parcel_id:str)->tuple[dict,list[dict]]:
    if parcel_id in _CACHE:
        result,failures=_CACHE[parcel_id]
        out=dict(result);out['lojic_cache_hit']=True
        return out,[dict(x) for x in failures]
    failures=[];result={'parcel_type':None,'lot_sqft':None,'lot_acres':None,'pin':None,'zoning_code':None,'zoning_name':None,'zoning_type':None,'landuse_name':None,'lojic_parcel_verified':False,'lojic_cache_hit':False,'lojic_recovery':{}}
    try:
        escaped=parcel_id.replace("'","''")
        params={'where':f"PARCELID='{escaped}'",'outFields':'PARCELID,PARCEL_TYPE,PIN,SHAPE.AREA','returnGeometry':'true','returnCentroid':'true','outSR':'2246','f':'json'}
        p,meta=_request('GET',PVA_QUERY,params=params);result['lojic_recovery']['parcel']=meta
        feats=p.get('features') or []
        if not feats:
            failures=[{'source':'lojic_parcel','reason':'parcel_not_found'}];_CACHE[parcel_id]=(dict(result),list(failures));return result,failures
        feat=feats[0];a=feat.get('attributes') or {};sqft=a.get('SHAPE.AREA')
        try:sqft=float(sqft) if sqft is not None else None
        except Exception:sqft=None
        result.update({'parcel_type':a.get('PARCEL_TYPE'),'lot_sqft':round(sqft,1) if sqft is not None else None,'lot_acres':round(sqft/43560.0,4) if sqft is not None else None,'pin':a.get('PIN'),'lojic_parcel_verified':True})
        point=_representative_point(feat)
        try:
            z,zm=_point_layer(ZONING_QUERY,point,'ZONING_CODE,ZONING_NAME,ZONING_TYPE');result['lojic_recovery']['zoning']=zm
            if z:result.update({'zoning_code':clean(z.get('ZONING_CODE')) or None,'zoning_name':clean(z.get('ZONING_NAME')) or None,'zoning_type':clean(z.get('ZONING_TYPE')) or None})
            else:failures.append({'source':'lojic_zoning','reason':'no_intersection'})
        except Exception as exc:failures.append({'source':'lojic_zoning','reason':f'{type(exc).__name__}:{exc}'})
        try:
            l,lm=_point_layer(LANDUSE_QUERY,point,'LANDUSE_NAME');result['lojic_recovery']['landuse']=lm
            if l:result['landuse_name']=clean(l.get('LANDUSE_NAME')) or None
            else:failures.append({'source':'lojic_landuse','reason':'no_intersection'})
        except Exception as exc:failures.append({'source':'lojic_landuse','reason':f'{type(exc).__name__}:{exc}'})
    except Exception as exc:
        failures.append({'source':'lojic_parcel','reason':f'{type(exc).__name__}:{exc}'})
    _CACHE[parcel_id]=(dict(result),[dict(x) for x in failures])
    return result,failures
