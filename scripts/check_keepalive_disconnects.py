r"""
Matches server-side keep-alive disconnects in the BA-Broker log to flood
clients that lost more messages than failed to publish (QoS 2, 200 clients,
rep 1 and rep 3 of the 2026-11-02 flood matrix).

Run from the repo root:
    python scripts\check_keepalive_disconnects.py results\flood_server_logs\babroker_server.log
"""
import re,sys,pandas as pd
from pathlib import Path
lines=open(sys.argv[1],encoding='utf-8').read().splitlines()
conn={}; last=None; ev=[]
for l in lines:
    t=re.search(r'T(\d\d:\d\d:\d\d)',l); t=t.group(1) if t else ''
    m=re.search(r'connection from ([\d.]+:\d+)',l)
    if m: last=m.group(1); continue
    m=re.search(r'CONNECT id=\W*([\w-]+)',l)
    if m and last: conn[last]=m.group(1); continue
    m=re.search(r'keep-alive timeout for ([\d.]+:\d+)',l)
    if m: ev.append((t,'ka',conn.get(m.group(1),'?')))
    if 'i/o timeout' in l: ev.append((t,'io',l))
for r,(a,b) in {1:('23:01:00','23:01:55'),3:('23:37:20','23:38:25')}.items():
    e=[x for x in ev if a<=x[0]<=b]
    ka=sorted({x[2] for x in e if x[1]=='ka'}); io=[x for x in e if x[1]=='io']
    df=pd.concat([pd.read_csv(f) for f in Path(f'results/flood_babroker_qos2_n200_rep{r}').glob('flood_client*.csv')])
    bad=set(df[df.missing>df.publish_failures].client_id.astype(str))
    print(f'rep{r}: keepalive={len(ka)} sub={sum(k.endswith("-sub") for k in ka)} pub={sum(k.endswith("-pub") for k in ka)} io-timeouts={len(io)}')
    print('  missing>pubfail clients:',len(bad),'| of these with sub keepalive:',len([x for x in bad if x+'-sub' in ka]))
    print('  ex bad:',sorted(bad)[:4],'ex ka:',ka[:4])
    for x in io[:3]: print('  io:',x[2][:160])
