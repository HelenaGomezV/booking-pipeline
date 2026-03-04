"""
🏨 Booking Analytics Dashboard
Streamlit + Plotly interactive dashboard
Replicates Bronze → Silver → Gold pipeline results
Author: Helena Gómez Villegas
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import hashlib, os

# ── Config ───────────────────────────────────────────────────
st.set_page_config(page_title="Booking Analytics", page_icon="🏨", layout="wide")

# ── CSS (dark, devesh-style) ─────────────────────────────────
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
:root{--bg:#0E1117;--card:#161B22;--border:#30363D;--accent:#F59E0B;--text:#E6EDF3;--muted:#8B949E;}
.main .block-container{padding:1rem 2rem 2rem;max-width:1500px}
h1,h2,h3{font-family:'Inter',sans-serif!important}

.kpi-row{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin:0 0 20px 0}
.kpi{background:var(--card);border:1px solid var(--border);
border-radius:10px;padding:14px 18px;position:relative;overflow:hidden}
.kpi::after{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.kpi.c1::after{background:#92400E} .kpi.c2::after{background:#64748B}
.kpi.c3::after{background:#F59E0B} .kpi.c4::after{background:#EF4444}
.kpi.c5::after{background:#3B82F6} .kpi.c6::after{background:#10B981}
.kpi-label{font-size:.65rem;text-transform:uppercase;letter-spacing:1.5px;
color:var(--muted);font-family:'Inter',sans-serif;font-weight:600}
.kpi-val{font-size:1.55rem;font-weight:700;color:var(--text);
font-family:'JetBrains Mono',monospace;margin:2px 0}
.kpi-sub{font-size:.7rem;color:var(--muted);font-family:'Inter',sans-serif}

.sec{font-family:'Inter',sans-serif;font-size:1rem;font-weight:700;
color:var(--text);margin:14px 0 6px;display:flex;align-items:center;gap:8px}
.tag{font-family:'JetBrains Mono',monospace;font-size:.55rem;padding:2px 7px;
border-radius:4px;letter-spacing:.8px;font-weight:600}
.tag-g{background:#78350F;color:#FDE68A}
.tag-s{background:#1E293B;color:#94A3B8}

section[data-testid="stSidebar"]{background:#0B0F19;border-right:1px solid var(--border)}
.stTabs [data-baseweb="tab-list"]{gap:6px;background:var(--card);border-radius:8px;padding:5px;border:1px solid var(--border)}
.stTabs [data-baseweb="tab"]{color:var(--muted)!important;font-weight:500;border-radius:6px;padding:8px 16px!important}
.stTabs [aria-selected="true"]{background:#1E40AF!important;color:#fff!important;border-radius:6px}
#MainMenu,footer,header{visibility:hidden}
</style>""", unsafe_allow_html=True)

# ── Data Pipeline ────────────────────────────────────────────
@st.cache_data(show_spinner="🔄 Running pipeline: Bronze → Silver → Gold ...")
def pipeline():
    for p in ["data/raw/bookings.csv","../data/raw/bookings.csv"]:
        if os.path.exists(p): csv=p; break
    else:
        st.error("❌ bookings.csv not found in data/raw/"); st.stop()

    raw=pd.read_csv(csv,dtype=str); bronze_n=len(raw)
    cols=raw.columns.tolist()
    raw["_h"]=raw.apply(lambda r:hashlib.sha256("|".join(str(r.get(c,""))for c in cols).encode()).hexdigest(),axis=1)
    df=raw.drop_duplicates("_h",keep="first").copy(); dupes=bronze_n-len(df)
    df=df.replace({"NULL":pd.NA,"NA":pd.NA})

    for c in["is_canceled","booking_to_arrival_time","arrival_date_year","arrival_date_week_number",
             "arrival_date_day_of_month","stays_in_weekend_nights","stays_in_week_nights","adults",
             "children","babies","is_repeated_guest","previous_cancellations",
             "previous_bookings_not_canceled","booking_changes","days_in_waiting_list",
             "parking_lot","total_of_special_requests"]:
        df[c]=pd.to_numeric(df[c],errors="coerce").astype("Int64")
    df["adr"]=pd.to_numeric(df["adr"],errors="coerce")
    df=df.rename(columns={"hotel":"hotel_type","booking_to_arrival_time":"lead_time"})
    df["stays_in_weekend_nights"]=df["stays_in_weekend_nights"].fillna(0)
    df["stays_in_week_nights"]=df["stays_in_week_nights"].fillna(0)
    df["total_nights"]=df["stays_in_weekend_nights"]+df["stays_in_week_nights"]
    df["revenue"]=df.apply(lambda r:float(r["adr"]or 0)*float(r["total_nights"])if r["is_canceled"]==0 else 0.0,axis=1)

    mm=dict(January="01",February="02",March="03",April="04",May="05",June="06",
            July="07",August="08",September="09",October="10",November="11",December="12")
    df["mn"]=df["arrival_date_month"].map(mm)
    df["year_month"]=df["arrival_date_year"].astype(str)+"-"+df["mn"].astype(str)
    df["arrival_date"]=pd.to_datetime(df["arrival_date_year"].astype(str)+"-"+df["mn"].astype(str)+"-"+df["arrival_date_day_of_month"].astype(str),errors="coerce")
    df["was_room_changed"]=df["reserved_room_type"]!=df["assigned_room_type"]

    # Gold: countries
    nc=df[df["is_canceled"]==0]
    ct=nc[nc["country"].notna()].groupby("country").agg(
        bookings=("country","count"),revenue=("revenue","sum"),adr=("adr","mean"),
        nights=("total_nights","mean"),repeat=("is_repeated_guest",lambda x:round(100*x.sum()/len(x),1))
    ).reset_index().sort_values("bookings",ascending=False).reset_index(drop=True)
    ct["rank"]=ct["bookings"].rank(ascending=False,method="min").astype(int)
    ct["share"]=round(100*ct["bookings"]/ct["bookings"].sum(),1)

    # Gold: MoM
    mo=df.groupby(["hotel_type","year_month"]).agg(
        total=("hotel_type","count"),rev=("revenue","sum"),avg_adr=("adr","mean"),
        ok=("is_canceled",lambda x:(x==0).sum()),cx=("is_canceled",lambda x:(x==1).sum())
    ).reset_index().sort_values(["hotel_type","year_month"])
    mo["prev"]=mo.groupby("hotel_type")["rev"].shift(1)
    mo["mom"]=mo.apply(lambda r:round(100*(r["rev"]-r["prev"])/r["prev"],1)if pd.notna(r["prev"])and r["prev"]>0 else None,axis=1)

    return dict(df=df,bronze=bronze_n,silver=len(df),dupes=dupes,ct=ct,mo=mo,nc=nc)

D=pipeline(); df=D["df"]; ct=D["ct"]; mo=D["mo"]; nc=D["nc"]
T="plotly_dark"; CM={"Hotel":"#EF4444","Apartment":"#3B82F6"}
def fig_layout(f,h=340):
    f.update_layout(height=h,template=T,margin=dict(l=10,r=10,t=10,b=30),
    plot_bgcolor="rgba(0,0,0,0)",paper_bgcolor="rgba(0,0,0,0)")
    return f

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏨 Booking Pipeline")
    st.caption("Medallion Architecture · Interactive Dashboard")
    st.divider()
    st.markdown("**Pipeline Stats**")
    a,b=st.columns(2)
    a.metric("Bronze",f"{D['bronze']:,}"); b.metric("Dupes",f"{D['dupes']:,}")
    a.metric("Silver",f"{D['silver']:,}"); b.metric("Countries",f"{len(ct)}")
    st.divider()
    st.markdown("**Filters**")
    hf=st.multiselect("Hotel Type",df["hotel_type"].dropna().unique().tolist(),default=df["hotel_type"].dropna().unique().tolist())
    yrs=sorted(df["arrival_date_year"].dropna().unique().tolist())
    yf=st.multiselect("Year",yrs,default=yrs)
    sf=st.radio("Status",["All","Completed","Canceled"],horizontal=True)
    st.divider()
    st.caption("Helena Gómez Villegas · Data Engineer")

filt=df[(df["hotel_type"].isin(hf))&(df["arrival_date_year"].isin(yf))]
if sf=="Completed":filt=filt[filt["is_canceled"]==0]
elif sf=="Canceled":filt=filt[filt["is_canceled"]==1]

# ── Header ───────────────────────────────────────────────────
st.markdown("<h1 style='background:linear-gradient(90deg,#F59E0B,#F97316);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:2rem;margin-bottom:0'>🏨 Booking Analytics Dashboard</h1>",unsafe_allow_html=True)
st.caption("BRONZE → SILVER → GOLD · MEDALLION ARCHITECTURE · INTERACTIVE RESULTS")

# ── KPIs ─────────────────────────────────────────────────────
rev=nc["revenue"].sum(); cr=100*(df["is_canceled"]==1).sum()/len(df)
adr_v=df["adr"].mean(); ng=nc["total_nights"].mean(); rr=100*(df["is_repeated_guest"]==1).sum()/len(df)
st.markdown(f"""<div class="kpi-row">
<div class="kpi c1"><div class="kpi-label">CSV Rows</div><div class="kpi-val">{D['bronze']:,}</div><div class="kpi-sub">raw input</div></div>
<div class="kpi c2"><div class="kpi-label">Unique</div><div class="kpi-val">{D['silver']:,}</div><div class="kpi-sub">−{D['dupes']:,} dupes</div></div>
<div class="kpi c3"><div class="kpi-label">Revenue</div><div class="kpi-val">€{rev/1e6:.1f}M</div><div class="kpi-sub">non-canceled</div></div>
<div class="kpi c4"><div class="kpi-label">Cancel Rate</div><div class="kpi-val">{cr:.1f}%</div><div class="kpi-sub">1 in 3</div></div>
<div class="kpi c5"><div class="kpi-label">Avg ADR</div><div class="kpi-val">€{adr_v:.0f}</div><div class="kpi-sub">per night</div></div>
<div class="kpi c6"><div class="kpi-label">Avg Stay</div><div class="kpi-val">{ng:.1f}n</div><div class="kpi-sub">{rr:.1f}% repeat</div></div>
</div>""",unsafe_allow_html=True)

# ── Tabs ─────────────────────────────────────────────────────
t1,t2,t3,t4,t5=st.tabs(["Overview","Countries","MoM Growth","Hotel vs Apt","Explorer"])

# ═══ TAB 1 ═══
with t1:
    a,b=st.columns(2)
    with a:
        st.markdown('<div class="sec">📊 Monthly Bookings</div>',unsafe_allow_html=True)
        m1=filt.groupby("year_month").size().reset_index(name="n").sort_values("year_month")
        f=px.area(m1,x="year_month",y="n",color_discrete_sequence=["#3B82F6"])
        f.update_traces(line_width=2.5,fill="tozeroy",fillcolor="rgba(59,130,246,.12)")
        st.plotly_chart(fig_layout(f),use_container_width=True)
    with b:
        st.markdown('<div class="sec">💰 Revenue by Month <span class="tag tag-g">GOLD</span></div>',unsafe_allow_html=True)
        m2=nc.groupby("year_month")["revenue"].sum().reset_index().sort_values("year_month")
        st.plotly_chart(fig_layout(px.bar(m2,x="year_month",y="revenue",color_discrete_sequence=["#F59E0B"])),use_container_width=True)
    a,b,c=st.columns(3)
    with a:
        st.markdown('<div class="sec">❌ Cancel by Type</div>',unsafe_allow_html=True)
        cx=df.groupby("hotel_type")["is_canceled"].agg(["sum","count"]).reset_index()
        cx["r"]=(100*cx["sum"]/cx["count"]).round(1)
        f3=px.bar(cx,x="hotel_type",y="r",color="hotel_type",text="r",color_discrete_map=CM)
        f3.update_traces(texttemplate="%{text:.1f}%",textposition="outside")
        f3.update_layout(showlegend=False)
        st.plotly_chart(fig_layout(f3,300),use_container_width=True)
    with b:
        st.markdown('<div class="sec">🍽️ Board Type</div>',unsafe_allow_html=True)
        bd=filt["board"].value_counts().reset_index();bd.columns=["b","n"]
        f4=px.pie(bd,names="b",values="n",hole=.55,color_discrete_sequence=["#F59E0B","#3B82F6","#10B981","#EF4444","#8B5CF6"])
        f4.update_layout(legend=dict(font_size=10))
        st.plotly_chart(fig_layout(f4,300),use_container_width=True)
    with c:
        st.markdown('<div class="sec">📅 Lead Time</div>',unsafe_allow_html=True)
        st.plotly_chart(fig_layout(px.histogram(filt,x="lead_time",nbins=50,color_discrete_sequence=["#14B8A6"]),300),use_container_width=True)

# ═══ TAB 2 ═══
with t2:
    st.markdown('<div class="sec">🌍 popular_countries <span class="tag tag-g">GOLD</span></div>',unsafe_allow_html=True)
    tn=st.slider("Top N",5,50,20)
    top=ct.head(tn)
    a,b=st.columns([3,2])
    with a:
        f5=px.bar(top.sort_values("bookings"),x="bookings",y="country",orientation="h",color="revenue",
                  text="share",color_continuous_scale="YlOrBr")
        f5.update_traces(texttemplate="%{text:.1f}%",textposition="outside")
        f5.update_layout(coloraxis_colorbar_title="Rev €")
        st.plotly_chart(fig_layout(f5,max(450,tn*26)),use_container_width=True)
    with b:
        st.dataframe(top[["rank","country","bookings","revenue","adr","nights","repeat","share"]].rename(
            columns={"rank":"#","country":"Country","bookings":"Bookings","revenue":"Revenue","adr":"ADR",
                     "nights":"Nights","repeat":"Repeat%","share":"Share%"}
        ).style.format({"Revenue":"€{:,.0f}","ADR":"€{:.0f}","Nights":"{:.1f}","Repeat%":"{:.1f}%","Share%":"{:.1f}%"}),
        height=max(450,tn*26),use_container_width=True)

    st.markdown('<div class="sec">🗺️ Revenue Treemap (Top 30)</div>',unsafe_allow_html=True)
    ft=px.treemap(ct.head(30),path=["country"],values="revenue",color="bookings",color_continuous_scale="YlOrBr")
    st.plotly_chart(fig_layout(ft,380),use_container_width=True)

# ═══ TAB 3 ═══
with t3:
    st.markdown('<div class="sec">📈 mom_growth <span class="tag tag-g">GOLD</span></div>',unsafe_allow_html=True)
    hc=st.radio("Type",["Both","Hotel","Apartment"],horizontal=True,key="mr")
    mf=mo if hc=="Both"else mo[mo["hotel_type"]==hc]
    a,b=st.columns(2)
    with a:
        st.markdown("**Revenue Trend**")
        f6=px.line(mf,x="year_month",y="rev",color="hotel_type",color_discrete_map=CM,markers=True)
        f6.update_layout(legend_title="",yaxis_title="Revenue €")
        st.plotly_chart(fig_layout(f6,380),use_container_width=True)
    with b:
        st.markdown("**MoM % Change**")
        mp=mf.dropna(subset=["mom"])
        f7=go.Figure()
        for ht in mp["hotel_type"].unique():
            d=mp[mp["hotel_type"]==ht]
            clr=["#10B981"if v>=0 else"#EF4444"for v in d["mom"]]
            f7.add_trace(go.Bar(x=d["year_month"],y=d["mom"],name=ht,
                marker_color=clr if hc!="Both"else CM.get(ht,"#F59E0B")))
        f7.update_layout(barmode="group",yaxis_title="MoM %")
        st.plotly_chart(fig_layout(f7,380),use_container_width=True)

    st.markdown("**Full Data**")
    md=mf[["hotel_type","year_month","total","rev","ok","cx","mom"]].copy()
    md.columns=["Type","Month","Total","Revenue €","Completed","Canceled","MoM %"]
    st.dataframe(md.style.format({"Revenue €":"€{:,.0f}","MoM %":"{:+.1f}%"}),use_container_width=True,height=400)

# ═══ TAB 4 ═══
with t4:
    st.markdown('<div class="sec">🏨 Hotel vs Apartment Deep Dive</div>',unsafe_allow_html=True)
    comp=df.groupby("hotel_type").agg(n=("hotel_type","count"),cx=("is_canceled","sum"),
        adr_m=("adr","mean"),ng_m=("total_nights","mean"),rev=("revenue","sum"),
        lead_m=("lead_time","mean"),chg=("was_room_changed","sum"),rpt=("is_repeated_guest","sum")).reset_index()
    comp["cx_r"]=(100*comp["cx"]/comp["n"]).round(1)
    comp["chg_r"]=(100*comp["chg"]/comp["n"]).round(1)
    comp["rpt_r"]=(100*comp["rpt"]/comp["n"]).round(1)

    a,b,c=st.columns(3)
    with a:
        f8=px.bar(comp,x="hotel_type",y="n",color="hotel_type",text="n",color_discrete_map=CM)
        f8.update_traces(texttemplate="%{text:,}",textposition="outside")
        f8.update_layout(showlegend=False,title="Total Bookings")
        st.plotly_chart(fig_layout(f8,300),use_container_width=True)
    with b:
        f9=px.bar(comp,x="hotel_type",y="rev",color="hotel_type",text="rev",color_discrete_map=CM)
        f9.update_traces(texttemplate="€%{text:,.0f}",textposition="outside")
        f9.update_layout(showlegend=False,title="Total Revenue")
        st.plotly_chart(fig_layout(f9,300),use_container_width=True)
    with c:
        mt=comp.melt("hotel_type",["cx_r","chg_r","rpt_r"],"metric","pct")
        mt["metric"]=mt["metric"].map({"cx_r":"Cancel %","chg_r":"Room Change %","rpt_r":"Repeat %"})
        f10=px.bar(mt,x="metric",y="pct",color="hotel_type",barmode="group",text="pct",color_discrete_map=CM)
        f10.update_traces(texttemplate="%{text:.1f}%",textposition="outside")
        f10.update_layout(title="Key Rates",legend_title="")
        st.plotly_chart(fig_layout(f10,300),use_container_width=True)

    st.markdown('<div class="sec">💰 ADR Distribution</div>',unsafe_allow_html=True)
    f11=px.histogram(filt,x="adr",color="hotel_type",nbins=80,barmode="overlay",color_discrete_map=CM,opacity=.7)
    f11.update_layout(xaxis_title="ADR (€)",yaxis_title="Count")
    st.plotly_chart(fig_layout(f11,320),use_container_width=True)

    st.markdown('<div class="sec">📅 Seasonality: Bookings by Month</div>',unsafe_allow_html=True)
    sea=filt.groupby(["hotel_type","arrival_date_month"]).size().reset_index(name="n")
    mo_order=["January","February","March","April","May","June","July","August","September","October","November","December"]
    sea["arrival_date_month"]=pd.Categorical(sea["arrival_date_month"],categories=mo_order,ordered=True)
    sea=sea.sort_values("arrival_date_month")
    f12=px.line(sea,x="arrival_date_month",y="n",color="hotel_type",color_discrete_map=CM,markers=True)
    f12.update_layout(xaxis_title="",yaxis_title="Bookings",legend_title="")
    st.plotly_chart(fig_layout(f12,320),use_container_width=True)

# ═══ TAB 5 ═══
with t5:
    st.markdown('<div class="sec">🔎 Silver Layer Explorer <span class="tag tag-s">SILVER</span></div>',unsafe_allow_html=True)
    st.caption(f"Showing {len(filt):,} rows after filters")
    sc=st.multiselect("Columns",filt.columns.tolist(),
        default=["hotel_type","is_canceled","country","adr","total_nights","revenue","arrival_date","year_month","lead_time","market_segment","deposit_type"])
    st.dataframe(filt[sc].head(500),use_container_width=True,height=500)
    a,b=st.columns(2)
    with a:
        st.markdown("**Numeric Summary**")
        st.dataframe(filt[["adr","total_nights","revenue","lead_time"]].describe().round(2),use_container_width=True)
    with b:
        st.markdown("**Value Counts**")
        cc=st.selectbox("Column",["hotel_type","country","market_segment","deposit_type","customer_type","board","acquisition_channel","reservation_status"])
        vc=filt[cc].value_counts().head(20).reset_index();vc.columns=[cc,"n"]
        st.plotly_chart(fig_layout(px.bar(vc,x=cc,y="n",color_discrete_sequence=["#F59E0B"]),300),use_container_width=True)
