# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 17:04:39 2022

@author: zpfundisql
"""

from urllib.request import urlopen as uReq
import urllib
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import datetime as dt
from contextlib import suppress
from sqlalchemy import create_engine
import pyodbc
#%%
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

ymd_str=dt.date.today().strftime('%Y%m%d')
print(ymd_str)
#%%
##upload to temp table
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                         "SERVER=JWTCVPMEDB13;"
                                         "DATABASE=Fundamentals;"
                                         "trusted_connection=yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

SQL_READ='''
select fcst.DT, fcst.HE, fcst.region as Region
,  round(LD,0) as LD
,round(ac.Demand,0)  as LC_act
,round(fcst.Hydro,0) as HY
,case when WAT is null then Hydro_a else WAT end as HY_act
,case when Wind is null then WND_ot else Wind end as Wind
,case when WND is null then Wind_a else WND end as Wind_act
,case when Solar is null then SUN_ot else Solar end as Solar
,case when SUN is null then Solar_a else SUN end as Solar_act
, case when NUC is null then NUC_PF else NUC end as Nuke
, case when COL is null then COL_PF else COL end as Coal
, case when OTH is null then OTH_PF else OTH end as Oth
, case when IT is null then IT_pf else IT end as IT
, case when NG is null then NG_PF else NG end as NG
,CDD
,HDD



from
--fcst.DT, fcst.HE, fcst.region, Case when  from 
(


select r.*,ld.mwh as Ld,CDD,HDD from 
(
	select DT, HE, Region,sum(Hydro) as Hydro, sum(Hydro_A) Hydro_a,sum(Wind) as Wind, sum(Wind_a) as Wind_a,sum(Solar) as Solar,sum(Solar_A) as Solar_a  
	from(
			select d.DT, d.HE, case when d.LC in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE','GRD') then 'PNW' when d.LC in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
			when d.LC in ('BAN','TID','LDW','IID') then 'Cal-nonISO' when d.LC in ('GWA','WWA','WAU','NWM','IPC','NEV','PAC','WAC','PSC') then 'MTW' else 'Other' end as Region, h.H_Fcst as Hydro, h.Act as Hydro_A
			,w.Fcst as Wind, w.act as Wind_a,S.Fcst as Solar, s.MW as Solar_A 

			from(
			select case when BA = 'PACW' then 'PCW' 
			 when BA = 'GRID' then 'GRD' 
			else  left(BA,3) end as LC,BA, DT, HE
				from(
				select distinct BA, 1 as lnk 
				from 
				dbo.LKP_EIA_BA_hrly )b 
				join(
				select distinct DT, HE, 1 as lnk from dbo.E_Hydro_Fcst
				where DT > getdate()-28)a on a.lnk=b.lnk)d

				left outer join 

			(	 
				 select DT, HE, LC,case when fcst<0   then 0 else round(fcst,0) end as Fcst, convert(float,MW) as act, pred
															--,case when DT> getdate()-7 then 'LastW' else 'Hist' end as 'Period'
															from( 

															Select e.DT, e.HE, e.LC,  
															isnull((c.Wind_1*e.Wind_1),0)+isnull((c.Wind_2*e.Wind_2),0)+isnull((c.Wind_3*e.Wind_3),0)+isnull((c.Wind_4*e.Wind_4),0)
															+Intercept 

															as fcst
															,ln.MW,ln.pred

															from (
																			select DT,HB+1 as HE,LC, avg(case when LN = 1 then Wind end) as 'Wind_1'
																			, avg(case when LN = 2 then Wind end) as 'Wind_2'
																			, avg(case when LN = 3 then Wind end) as 'Wind_3'
																			, avg(case when LN = 4 then Wind end) as 'Wind_4'


																			from(

																				select DT, HB, LOC
																				  ,left(LOC,3) as LC
																				  ,right(LOC,1) as LN, MW as Wind
																				FROM(
																					select dt, HB,convert(float,[Value]) as MW, Met, Loc
																					from 
																					dbo.NOAA_HRLY_LATLONG_WX_FC a
																					join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																					join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																					where Met = 'Wind'
																					and right(Loc,3) like 'W%'
																					--or a.locID < 7 and Met = 'Wind'
																					)a
																					)d
																			group by DT, HB,LC 
																			union all 
																			--integrate legacy weather stations' 


																														select distinct DT, HB+1 as HE, 'AVR' as Loc, avg(case when loc = 'Goodnoe Hill' then Wind end) as '1' 
																										, avg(case when loc = 'Roosevelt' then Wind end) as '2'
																										, avg(case when loc = 'Wasco' then Wind end) as '3'  
																										, null as '4'   
																											from(		
																													select DT, HB, LOC
																															  ,loc as LC
																															  ,1 as LN, MW as Wind
																															FROM(
																																select dt, HB,convert(float,[Value]) as MW, Met, Loc
																																from 
																																dbo.NOAA_HRLY_LATLONG_WX_FC a
																																join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																																join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																																where Met = 'Wind'
																																and a.locID in (0,1,2,3,4,5,6,116)
																																and DT> '2019-01-22'
																																--or (a.LocID = 116 and Met = 'Wind')

																																)a)b
																										group by DT,HB
																										--order by DT
																										union all 

																										select distinct DT, HB+1 as HE, 'BPA' as Loc, avg(case when loc = 'Goodnoe Hill' then Wind end) as '1' 
																										, avg(case when loc = 'Roosevelt' then Wind end) as '2'
																										, avg(case when loc = 'Tuc_W_1' then Wind end) as '3'  
																										, avg(case when loc = 'Vanscyle,KUJ/Butler' then Wind end) as '4'   
																											from(		
																													select DT, HB, LOC
																															  ,loc as LC
																															  ,1 as LN, MW as Wind
																															FROM(
																																select dt, HB,convert(float,[Value]) as MW, Met, Loc
																																from 
																																dbo.NOAA_HRLY_LATLONG_WX_FC a
																																join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																																join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																																where Met = 'Wind'
																																and a.locID in (0,1,2,3,4,5,6,116)
																																and DT> '2019-01-22'
																																--or (a.LocID = 116 and Met = 'Wind')

																																)a)b
																										group by DT,HB
																										--order by DT

																										union all 
																										select distinct DT, HB+1 as HE, 'PCW' as Loc, avg(case when loc = 'Goodnoe Hill' then Wind end) as '1' 
																										, avg(case when loc = 'Roosevelt' then Wind end) as '2'
																										, avg(case when loc = 'Tuc_W_1' then Wind end) as '3'  
																										, avg(case when loc = 'Vanscyle,KUJ/Butler' then Wind end) as '4'   
																											from(		
																													select DT, HB, LOC
																															  ,loc as LC
																															  ,1 as LN, MW as Wind
																															FROM(
																																select dt, HB,convert(float,[Value]) as MW, Met, Loc
																																from 
																																dbo.NOAA_HRLY_LATLONG_WX_FC a
																																join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																																join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																																where Met = 'Wind'
																																and a.locID in (0,1,2,3,4,5,6,116)
																																and DT> '2019-01-22'
																												
																																--or (a.LocID = 116 and Met = 'Wind')

																																)a)b
																										group by DT,HB


																											union all 
																										select distinct DT, HB+1 as HE, 'WWA' as Loc, avg(case when loc = 'NWMT_W_1' then Wind end) as '1' 
																										, avg(case when loc = 'NWMT_W_2' then Wind end) as '2' 
																										,null as '3'
																										, null as '4'
																											from(		
																													select DT, HB, LOC
																															  ,loc as LC
																															  ,1 as LN, MW as Wind
																															FROM(
																																select dt, HB,convert(float,[Value]) as MW, Met, Loc
																																from 
																																dbo.NOAA_HRLY_LATLONG_WX_FC a
																																join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																																join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																																where Met = 'Wind'
																																and a.locID in (97,98)
																																and DT> '2019-01-22'
																																--or (a.LocID = 116 and Met = 'Wind')

																																)a)b
																										group by DT,HB
																										union all 
																										select distinct DT, HB+1 as HE, 'GWA' as Loc, avg(case when loc = 'NWMT_W_1' then Wind end) as '1' 
																										, avg(case when loc = 'NWMT_W_2' then Wind end) as '2' 
																										,null as '3'
																										, null as '4'
																											from(		
																													select DT, HB, LOC
																															  ,loc as LC
																															  ,1 as LN, MW as Wind
																															FROM(
																																select dt, HB,convert(float,[Value]) as MW, Met, Loc
																																from 
																																dbo.NOAA_HRLY_LATLONG_WX_FC a
																																join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																																join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																																where Met = 'Wind'
																																and a.locID in (97,98)
																																and DT> '2019-01-22'
																													
																																--or (a.LocID = 116 and Met = 'Wind')

																																)a)b
																										group by DT,HB


																										union all 
																										select distinct DT, HB+1 as HE, 'LDW' as Loc, avg(case when loc = 'Goodnoe Hill' then Wind end) as '1' 
																										, avg(case when loc = 'Roosevelt' then Wind end) as '2'
																										, avg(case when loc = 'LDWP_S_2' then Wind end) as '3'  
																										, null as '4'   
																											from(		
																													select DT, HB, LOC
																															  ,loc as LC
																															  ,1 as LN, MW as Wind
																															FROM(
																																select dt, HB,convert(float,[Value]) as MW, Met, Loc
																																from 
																																dbo.NOAA_HRLY_LATLONG_WX_FC a
																																join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
																																join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
																																where Met = 'Wind'
																																and a.locID in (0,1,2,3,4,5,6,103)
																																and DT> '2019-01-22'
																																)a)b where DT> '2019-09-27'
																										group by DT,HB

															) e
															join 
															dbo.Wind_Coeffs c on c.Loc = e.LC 
															--left outer join  
															--dbo.BA_SOLAR_CAP f
															--on month(f.DM) = month(e.DT) and day(f.DM) = day(e.dt) and f.HE=e.HE and left(f.BA,3)=e.LC
															--left outer join 
															 --dbo.Solar_data t on t.DT=e.DT and t.HE=e.HE and t.LC=e.LC
													left outer join dbo.Wind_lm ln on ln.LC = e.LC and ln.HE = e.HE and ln.dt = e.DT
													)dd
										
										
										
							)w						on w.DT=d.DT and w.HE=d.he and d.LC=w.LC


			left outer join(



						select Fcst_Day, HE ,LC,avg(MW) as Act, avg(Hydro) as H_Fcst
							from(
								select Fcst_Day, HE, 'BPA' as LC, null as MW, sum(gen) as Hydro
									from(
									select Fcst_Day, HE, DN, Station,'All' as Region, Gen , RANK () over (partition by Fcst_Day, HE, Station Order by UpDN desc) as rnk
									from dbo.NWRFC_Gen_Fcst a
									join dbo.lkp_NWRFC_StationID b on a.station = b.stationID 
									) dd where rnk = 1 
				
									group by Fcst_Day, HE
				--	order by Fcst_Day desc, HE desc
									union all
								select DT, HE,Loc,Null,Fcst from dbo.E_Hydro_Fcst where Fcst > 0
								--order by Fcst_Day desc
					
					
								union all
								select DT,HE,case when BA = 'PACW' then 'PCW'  when BA = 'GRID' then 'GRD' else left(BA,3) end as BA,MW,null from EIA_BA_Gen2 a
								join LKP_EIA_BA_hrly b on b.BA_ID=a.BA_ID
								join LKP_EIA_Type_hrly c on c.Type_ID=a.Type_ID
								where Fuel_Type = 'WAT'
						) a group by Fcst_day, HE, LC

						) H on H.Fcst_Day=d.DT and h.HE=d.HE and h.LC=d.LC



						left outer join (
			
										select DT, HE, LC, CAP, round(Est_man,2) as Est_man,MW,case when Est_man*CAP <0   then 0 when Est_man > 1.15 then Cap*1.15 else round(Est_man*CAP,2) end as Fcst from( 

										Select e.DT, e.HE, e.LC, f.CAP, 
										isnull((c.precip_1*e.Precip_1),0)+isnull((c.precip_2*e.Precip_2),0)+isnull((c.precip_3*e.Precip_3),0)+isnull((c.precip_4*e.Precip_4),0)
											+
											isnull((c.Skyc_1*e.Skyc_1),0)+isnull((c.Skyc_2*e.Skyc_2),0)+isnull((c.Skyc_3*e.Skyc_3),0)+isnull((c.Skyc_4*e.Skyc_4),0)+Intercept as Est_man
											,t.MW

										from(
										select DT,HB+1 as HE,LC, avg(case when LN = 1 then precip end) as 'Precip_1'
										, avg(case when LN = 2 then precip end) as 'Precip_2'
										, avg(case when LN = 3 then precip end) as 'Precip_3'
										, avg(case when LN = 4 then precip end) as 'Precip_4'
										, avg(case when LN = 1 then Skyc end) as 'Skyc_1'
										, avg(case when LN = 2 then Skyc end) as 'Skyc_2'
										, avg(case when LN = 3 then Skyc end) as 'Skyc_3'
										, avg(case when LN = 4 then Skyc end) as 'Skyc_4'
										, avg(case when LN = 1 then Temp end) as 'Temp_1'
										, avg(case when LN = 2 then Temp end) as 'Temp_2'
										, avg(case when LN = 3 then Temp end) as 'Temp_3'
										, avg(case when LN = 4 then Temp end) as 'Temp_4'

										from(
											select DT, HB, LOC, avg(case when met = 'PrecipPtnl' then MW End) as 'Precip'
											 , avg(case when met = 'SkyCvr' then MW End) as 'Skyc'
											  , avg(case when met = 'Temp' then MW End) as 'Temp'
											  ,case when LOC in ('Cal _10','Cal _6') then 'IID' else left(LOC,3) end as LC
											  ,case when LOC = 'Cal _10' then 1 when LOC = 'Cal _6' then 2 else right(LOC,1) end as LN
											FROM(
												select dt, HB, convert(float,[Value]) as MW, Met, Loc 
												from 
												dbo.NOAA_HRLY_LATLONG_WX_FC a
												join dbo.lkpNOAA_HRLY_LATLONG_MET b on a.MetID = b.MetID
												join dbo.lkpNOAA_HRLY_LatLong_LOC c on c.LocID = a.locID
												where Met in ('PrecipPtnl','SkyCvr','Temp')
												and LOC in ('WACM3_S_2','WACM2_S_1','WACM_S_2','WACM_S_1','TEPC_S_2','TEPC_S_1','SRP_S_3','SRP_S_2','SRP_S_1','SPP_S_4','SPP_S_3','SPP_S_2','SPP_S_1','PSCO_S_4','PSCO_S_3','PSCO_S_2','PSCO_S_1',
											'PNM_S_2','PNM_S_1','PAC_S_5','PAC_S_4','PAC_S_3','PAC_S_2','PAC_S_1','NWMT_S_1','NEVP_S_4','NEVP_S_3','NEVP_S_2','NEVP_S_1','LADWP_S2','LADWP_S1','IPCO_S_2','IPCO_S_1','EPE_S_2','EPE_S_1','AZPS_S_6',
											'AZPS_S_5','AZPS_S_4','AZPS_S_3','AZPS_S_2','AZPS_S_1','LDWP_S_1','LDWP_S_2','BANC_S_1','Cal _6','Cal _10','LDWP_S_3','LDWP_S_4','PCW_S_1','PCW_S_2','AVA_S_1','PGE_S_1','PGE_S_2','PGE_S_3'))a
											group by DT,HB,Loc
										)d where DT > getdate() -7 

										group by DT, HB, LC) e
										join 
										dbo.solar_Coeffs c on c.Loc = e.LC and c.HE = e.HE 
										left outer join  
										dbo.BA_SOLAR_CAP f
										on month(f.DM) = month(e.DT) and day(f.DM) = day(e.dt) and f.HE=e.HE and left(f.BA,3)=e.LC
										left outer join 
										 dbo.Solar_data t on t.DT=e.DT and t.HE=e.HE and t.LC=e.LC)pp
			)s on s.DT=d.DT and s.HE = d.HE and s.LC=d.LC
	)ren group by DT, HE, Region

	--order by DT desc, HE desc, Region 

)r
left outer join (select * from (select fcstdate, case when region = 'CA' then 'Cal-nonISO' when region ='WEST' then 'MTW' else region end as Region,DT, HE, mwh, CDD,HDD   ,rank() over (partition by DT, HE, Region order by fcstDate desc) as rnk from dbo.RegionalLoadForecast)l where rnk=1) ld on r.DT = ld.DT and r.HE=LD.HE and r.region=ld.region
--where rnk = 1 

--order by r.DT desc, r.HE desc, r.Region
)fcst


left outer join(


select DT, HE,region
				, avg(case when fuel_type = 'Demand' then MW end) as 'Demand'
				, avg(case when fuel_type = 'Demand_FC' then MW end) as 'Demand_FC'
				, avg(case when fuel_type = 'WAT' then MW end) as 'WAT'
				, avg(case when fuel_type = 'NUC' then MW end) as 'NUC'
				, avg(case when fuel_type = 'OIL' then MW end) as 'OIL'
				, avg(case when fuel_type = 'WND' then MW end) as 'WND'
				, avg(case when fuel_type = 'COL' then MW end) as 'COL'
				, avg(case when fuel_type = 'OTH' then MW end) as 'OTH'
				, avg(case when fuel_type = 'SUN' then MW end) as 'SUN'
				, avg(case when fuel_type = 'NG' then MW end) as 'NG'
				, avg(case when fuel_type = 'Interchange' then MW end) as 'IT'
				
				from(
										select Region, DT, HE, Fuel_type, sum(MW) as MW from( 
										select *from (
												select 
												 case when 
												  b.BA = 'GRID' then 'PNW' when
												 left(BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE','GRD') then 'PNW' when left(B.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
												when BA = 'PACW'then 'PNW'
												 
												when left(B.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO' when left(B.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','PAC','WAC','PSC') then 'MTW' else 'Other' end as Region,
												a.DT, A.HE,left(B.BA,3) as lc,c.Fuel_Type,MW from dbo.EIA_BA_Gen2 a 
												join dbo.LKP_EIA_BA_hrly b on a.BA_ID=b.BA_ID
												join dbo.LKP_EIA_Type_hrly c on c.Type_ID=a.TYPE_ID
												where 
												--left(B.BA,3) in ('AZP','EPE','PNM','TEP','HGM','DEA','WAL','GRI','GRM','SRP')
												 DT between '2018-08-01' and getdate()-2
												 and b.BA not in ('NW','SW','CISO','CAL','SWPP')
											
										) a left outer join (select case when BA = 'PACW' then 'PCW'  when BA = 'GRID' then 'GRD' else left(BA,3) end as LOC, CAP from BA_Solar_MAXCAP)b on a.lc=b.LOC and a.Fuel_Type = 'SUN'
										where (MW < CAP*1.2 or CAP is null)
										--and (Fuel_Type = 'SUN' and MW < CAP*1.2 ) 
										and Fuel_Type <> 'Interchange'
										--order by MW desc
										)ddd group by Region, DT,HE,Fuel_Type
										--order by DT desc, HE desc
				union all 
									select BA_Region,DT, HE, 'Interchange' as Fuel_Type,  sum(MW) as MW
															from(
																		select DT,HE,MW,b.BA as BA, c.BA as CP 
																		,case when b.BA = 'PACW' then 'PNW'  when b.BA = 'GRID' then 'PNW' when left(b.BA,3) in ('AVA','AVR','BPA','PCW','GRD','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' when left(b.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
																					when left(b.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO'
																						WHEN b.BA = 'CISO' then 'CIS'  when left(b.BA,3) = 'SWP' then 'SPP' when left(b.BA,3) in ('AES','BCH','CFE') then 'CAD_MX' 
																					 when left(c.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','PAC','WAC','PSC') then 'MTW' else 'Other' end as BA_region
																		,case when C.BA = 'PACW' then 'PNW'  when c.BA = 'GRID' then 'PNW'  when left(C.BA,3) in ('AVA','AVR','BPA','PCW','GRD','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' when left(C.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
																					when left(C.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO'
																					WHEN c.BA = 'CISO' then 'CIS'  when left(c.BA,3) = 'SWP' then 'SPP' when left(c.BA,3) in ('AES','BCH','CFE') then 'CAD_MX' 
																					when left(c.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','PAC','WAC','PSC') then 'MTW' else 'Other'  end as CP_region
																		from dbo.EIA_BA_BA a
																		join dbo.LKP_EIA_BA_hrly b on a.BA_ID = b.BA_ID
																		join dbo.LKP_EIA_BA_hrly  c  on a.CP_ID=c.BA_ID
																		where b.BA not in ('NW','SW','CAL')
																		and C.BA not in ('NW','SW','CAL')
																		and a.DT between '2018-08-01' and getdate()-2
																		) a 
																	where BA_Region <> CP_region  
																	and BA_region <> 'CIS'
																	group by DT, HE, BA_region
																--	order by DT desc, HE desc
				
				
				
				
				
				
				
				)ot group by DT, HE,region

			



)ac on ac.DT=fcst.DT AND ac.HE = fcst.HE and ac.region = fcst.region
left outer join 
(



		select he,region
				, round(avg(case when fuel_type = 'NUC' then MW end),0) as 'NUC_pf'
				, round(avg(case when fuel_type = 'COL' then MW end),0) as 'COL_pf'
				, round(avg(case when fuel_type = 'NG' then MW end),0) as 'NG_pf'
				, round(avg(case when fuel_type = 'OTH' then MW end),0) as 'OTH_pf'
				, round(avg(case when fuel_type = 'IT' then MW end),0) as 'IT_pf'
				from(
						select region,DT, HE, Fuel_Type, sum(MW) as MW from (
						select * from(
									select 
									 case when left(BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE','GRD') then 'PNW' 
									 when BA = 'GRID' then 'PNW' 
									 when left(B.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
									when BA = 'PACW'then 'PNW'

									when left(B.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO' 
									
									when left(b.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','WAC','PSC') then 'MTW' 
									when BA = 'PACE' then 'MTW' 
									else 'Other'  end as Region,
									a.DT, A.HE,left(B.BA,3)  as lc,c.Fuel_Type,MW from dbo.EIA_BA_Gen2 a 
									join dbo.LKP_EIA_BA_hrly b on a.BA_ID=b.BA_ID
									join dbo.LKP_EIA_Type_hrly c on c.Type_ID=a.TYPE_ID
									where
									-- left(B.BA,3) in ('AZP','EPE','PNM','TEP','HGM','DEA','WAL','GRI','GRM','SRP')
									DT > getdate()-7
									and b.BA not in ('NW','SW','CISO','CAL','SWPP')
								) a left outer join (select case when BA = 'PACW' then 'PCW' else left(BA,3) end as LOC, CAP from BA_Solar_MAXCAP)b on a.lc=b.LOC and a.Fuel_Type = 'SUN'
						where (MW < CAP*1.2 or CAP is null)


							)dd group by Region, DT,HE, Fuel_Type
							union all 

							select BA_Region,DT, HE, 'IT' as Fuel_Type,  sum(MW) as MW
																		from(
																					select DT,HE,MW,b.BA as BA, c.BA as CP 
																					,case when b.BA = 'PACW' then 'PNW' 
																					 when b.BA = 'GRID' then 'PNW' 
																					when left(b.BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' when left(b.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
																								when left(b.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO'
																									WHEN b.BA = 'CISO' then 'CIS'  when left(b.BA,3) = 'SWP' then 'SPP' when left(b.BA,3) in ('AES','BCH','CFE') then 'CAD_MX' 
																								 else 'MTW' end as BA_region
																					,case when C.BA = 'PACW' then 'PNW' 
																					 when c.BA = 'GRID' then 'PNW' 
																					when left(C.BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' when left(C.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
																								when left(C.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO'
																								WHEN c.BA = 'CISO' then 'CIS' when left(c.BA,3) = 'SWP' then 'SPP' when left(c.BA,3) in ('AES','BCH','CFE') then 'CAD_MX' 
																								 when left(c.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','WAC','PSC') then 'MTW' 
																								 when c.BA = 'PACE' then 'MTW' 
																								 else 'Other'  end as CP_region
																					from dbo.EIA_BA_BA a
																					join dbo.LKP_EIA_BA_hrly b on a.BA_ID = b.BA_ID
																					join dbo.LKP_EIA_BA_hrly  c  on a.CP_ID=c.BA_ID
																					where b.BA not in ('NW','SW','CAL')
																					and C.BA not in ('NW','SW','CAL')
																					and a.DT>getdate()-7
																					) a 
																				where BA_Region <> CP_region  
																				and BA_region <> 'CIS'
																				group by DT, HE, BA_region


								--order by DT desc, HE desc
			)pf group by HE,region
) pf on  pf.HE = fcst.HE and pf.region = fcst.region


left outer join 
(

select HE,region
, round(avg(case when fuel_type = 'WAT' then MW end),0) as 'WAT_ot'
, round(avg(case when fuel_type = 'OIL' then MW end),0) as 'OIL_ot'
, round(avg(case when fuel_type = 'WND' then MW end),0) as 'WND_ot'
, round(avg(case when fuel_type = 'OTH' then MW end),0) as 'OTH_ot'
, round(avg(case when fuel_type = 'SUN' then MW end),0) as 'SUN_ot'
, round(avg(case when fuel_type = 'Interchange' then MW end),0) as 'IT_ot'

										from(
											select DT,HE, Region, Fuel_type, sum(MW) as MW
											from(
											select * from (	
															
															select 
															 case when left(BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' 
															 when BA = 'PACW'then 'PNW'
															  when b.BA = 'GRID' then 'PNW' 
															 when left(B.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
															when left(B.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO' 
															when left(b.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','WAC','PSC') then 'MTW' 
															when b.BA = 'PACE' then 'MTW' 
															else 'Other' 
															end as Region,
															a.DT, A.HE,left(B.BA,3) as lc,c.Fuel_Type,MW 
															
															from dbo.EIA_BA_Gen2 a 
															join dbo.LKP_EIA_BA_hrly b on a.BA_ID=b.BA_ID
															join dbo.LKP_EIA_Type_hrly c on c.Type_ID=a.TYPE_ID
															where 
															--left(B.BA,3) in ('AZP','EPE','PNM','TEP','HGM','DEA','WAL','GRI','GRM','SRP')
															 DT > getdate()-32
															 and b.BA not in ('NW','SW','CISO','CAL','SWPP')
															-- order by MW desc


												 ) a left outer join (select case when BA = 'PACW' then 'PCW' else left(BA,3) end as LOC, CAP from BA_Solar_MAXCAP)b on a.lc=b.LOC and a.Fuel_Type = 'SUN'
										where (MW between -100 and CAP*1.2 or CAP is null)
										and Fuel_Type <> 'Interchange'
												--and (MW>=0 and Fuel_Type <> 'Interchange')
											)a group by DT,HE, Region, Fuel_Type
										--	order by MW
							
								
								union all 
								
															 
																		select DT, HE, BA_Region,'Interchange' as Fuel_Type,  sum(MW) as MW
																		from(
																					select DT,HE,MW,b.BA as BA, c.BA as CP 
																					,case when b.BA = 'PACW' then 'PNW' 
																					 when b.BA = 'GRID' then 'PNW' when
																					left(b.BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' when left(b.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
																								when left(b.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO'
																									WHEN b.BA = 'CISO' then 'CIS'  when left(b.BA,3) = 'SWP' then 'SPP' when left(b.BA,3) in ('AES','BCH','CFE') then 'CAD_MX' 
																								 else 'MTW' end as BA_region
																					,case when C.BA = 'PACW' then 'PNW' 
																					 when c.BA = 'GRID' then 'PNW' 
																					when left(C.BA,3) in ('AVA','AVR','BPA','PCW','PSE','SCL','CHP','DOP','GCP','TPW','PGE') then 'PNW' when left(C.BA,3) in ('TEP','PNM','WAL','SRP','EPE','AZP','GRM','DEA','HGM','GRI') then 'DSW'
																								when left(C.BA,3) in ('BAN','TID','LDW','IID') then 'Cal-nonISO'
																								WHEN c.BA = 'CISO' then 'CIS' when left(c.BA,3) = 'SWP' then 'SPP' when left(c.BA,3) in ('AES','BCH','CFE') then 'CAD_MX' 
																								when left(c.BA,3) in ('GWA','WWA','WAU','NWM','IPC','NEV','WAC','PSC') then 'MTW' 
																									when C.BA = 'PACE' then 'MTW' 
																								 
																								 else 'Other'
																								  end as CP_region
																					from dbo.EIA_BA_BA a
																					join dbo.LKP_EIA_BA_hrly b on a.BA_ID = b.BA_ID
																					join dbo.LKP_EIA_BA_hrly  c  on a.CP_ID=c.BA_ID
																					where b.BA not in ('NW','SW','CAL')
																					and C.BA not in ('NW','SW','CAL')
																					and a.DT>getdate()-32
																					) a 
																				where BA_Region <> CP_region  
																				and BA_region <> 'CIS'
																				group by DT, HE, BA_region
																			


)o group by HE,Region 



										
)ot on ot.he = fcst.he and ot.region = fcst.region



--) aa where Region = 'DSW' and HE = '1'
--order by DT desc
order by fcst.DT ,fcst.HE ,fcst.REgion
'''
DF=pd.read_sql(SQL_READ, conn)


cursor = conn.cursor()
conn.close()
#%%

DF['IT'].loc[DF['Region']=='PNW']=DF['IT'].loc[DF['Region']=='PNW'].fillna(method='ffill')
DF['IT'].loc[DF['Region']=='MTW']=DF['IT'].loc[DF['Region']=='MTW'].fillna(method='ffill')
DF['Coal'].loc[DF['Region']=='MTW']=DF['Coal'].loc[DF['Region']=='MTW'].fillna(method='ffill')
DF['Nuke'].loc[DF['Region']=='PNW']=DF['Nuke'].loc[DF['Region']=='PNW'].fillna(method='ffill')
DF['NG'].loc[DF['Region']=='Cal-nonISO']=DF['NG'].loc[DF['Region']=='Cal-nonISO'].fillna(method='ffill')
DF['Coal'].loc[DF['Region']=='Cal-nonISO']=DF['Coal'].loc[DF['Region']=='Cal-nonISO'].fillna(method='ffill')
DF['Oth'].loc[DF['Region']=='Cal-nonISO']=DF['Oth'].loc[DF['Region']=='Cal-nonISO'].fillna(method='ffill')
#%%
print(DF)
#%%
conn = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=JWTCVPMEDB13;"
                                     "DATABASE=Fundamentals;"
                                     "trusted_connection=yes")
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                            "SERVER=JWTCVPMEDB13;"
                                             "DATABASE=Fundamentals;"
                                             "trusted_connection=yes")

engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))



DF.to_sql('WEST_Thermal2', engine, schema = 'dbo', index = False, if_exists='replace')
