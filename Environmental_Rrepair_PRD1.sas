PROC DATASETS library = WORK KILL nolist;
QUIT;

OPTION VALIDVARNAME=V7;

libname output '/appl/awr_data_p/output/environmental_repair';

DATA _null_;
 sysdt=today();
 call symput('cm',put(sysdt,yymmn6.));
 call symput('Todaydate', put(today(), yymmddd10.));
RUN;

%PUT &Todaydate.;
  
PROC SQL stimer feedback;
connect to odbc (&equator);
create table environmental_repair as
SELECT
   distinct *
  , case when Event_Lender_ID in (184720,184724,186651) then 'Testing'
    when Event_Lender_ID in (184707,184722,184728,238573) then 'Testing_bids'
    when Event_Lender_ID in (142854,184721,184852,184854,186652,186653,186654,186655,184725) then 'Remediation'
    when Event_Lender_ID in (184723,184729,184855) then 'Remediation_bids'
    else 'NA'
    end as TASK_CAT
FROM CONNECTION TO odbc
(
/* select only environmental order data */
WITH REO_DATE_FILTER
AS
(
  select
    distinct Property_ID
  	, event_type_ID
    , convert(date,Et.open_date,101) EO_OPEN_DT
    , CASE 
    WHEN ET.CLOSE_DATE IS NULL THEN convert(date, GETDATE(), 101)
    ELSE convert(date, ET.CLOSE_DATE, 101)
    END AS EO_CLOSE_DT
  from 
    ODS_Event_Tracking ET
  where 
    event_lender_id = '184705'
    and ET.ODS_Deleted_Flag = 'N'
    and (Et.close_date >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0) OR Et.close_date IS NULL)
  	and ET.task_status in (1,2,3,4,5)
)

select
  ET.ID,
  ET.event_type_ID,
  ET.Event_Lender_ID,
  Property.REO_Number REO_ID,
  Property.Loan_Number FNMN_LN_ID,
  Property.Address_1 ADDRESS,
  Property.City CITY,
  Property.State STATE,
  Property.county COUNTY,
  Property.Status_desc STATUS,
  ET.Target_PID,
  convert(date,Et.open_date,101) TASK_OPEN_DT,
  Et.open_date open_date_dt,
  EO.EO_OPEN_DT,
  convert(date,Et.close_date,101) TASK_CLOSE_DT,
  EO.EO_CLOSE_DT,
/* IF CLOSE DATE IS NULL THEN ASSIGN TODAY'S DATE TO FILTER DATA */
CASE WHEN ET.CLOSE_DATE IS NULL THEN convert(date,GETDATE(), 101)
  ELSE convert(date,ET.CLOSE_DATE, 101)
  END AS DATE_FILTER,

case when Et.close_date is NULL then DATEDIFF(DD,ET.open_date,getdate())
  else DATEDIFF(DD,ET.open_date,Et.close_date)
  end as DAYS_OPEN,

ET.Property_ID,

case when ET.task_status = 1 then 'Overdue'
  when ET.task_status = 2 then 'Warning'
  when ET.task_status = 3 then 'On Time'
  when ET.task_status = 4 then 'Completed'
  when ET.task_status = 5 then 'Closed(NC)'
  else 'NA' end as TASK_STATUS,

  Et.task_status as TASK_STATUS_n,
  Et.closed_task_status,
  remediation_bids.environ_recommendation_1 recomm_remediation1,
  remediation_bids.environ_recommendation_2 recomm_remediation2,
  remediation_bids.environ_recommendation_3 recomm_remediation3,
  testing_bids.environ_recommendation_1 recomm_testing1,
  testing_bids.environ_recommendation_2 recomm_testing2,
  testing_bids.environ_recommendation_3 recomm_testing3,
  review_test.environ_recommendation recomm_review_test,
  third_p_c.sel_environ_repairs_completed recomm_thirdp_cl,


lk_up_Issue_type.type_descr as EVENT_TYPE,
ET.complete_days Days_Overdue,
PersonAssigned.First_Name + ' ' + PersonAssigned.Last_Name Assigned_To,
PersonSalesRep.First_Name + ' ' + PersonSalesRep.Last_Name Sales_Rep,
SMGR_NAME.First_Name + ' ' + SMGR_NAME.Last_Name Sales_MGR,


vendor_name.VENDOR_ID as CONSULTANT_ID,
vendor_name.VENDOR_NAME AS VENDOR_NAME,
EVENT.event_name as EVENT_NAME

from ODS_Event_Tracking ET

left join ODS_Property Property
on 
ET.Property_ID = Property.Property_ID
and Property.ODS_Deleted_Flag = 'N'
and Property.Lender_ID = 1617

left join ODS_Person PersonAssigned
on 
ET.Target_PID = PersonAssigned.Person_ID
and PersonAssigned.ODS_Deleted_Flag = 'N'

left join ODS_Property_Role_Person PRPSalesRep
on 
Et.Property_ID = PRPSalesRep.Property_ID
and PRPSalesRep.Role_ID = 1268
and PRPSalesRep.ODS_Deleted_Flag = 'N'
and PRPSalesRep.Lender_ID = 1617

left join ODS_Person PersonSalesRep
on 
PRPSalesRep.Person_ID = PersonSalesRep.Person_ID
and PersonSalesRep.ODS_Deleted_Flag = 'N'

left join dbo.ODS_type_xref lk_up_Issue_type with(nolock)
on 
lk_up_Issue_type.type_id = ET.event_type_ID
and lk_up_issue_type.ODS_Deleted_Flag = 'N'

left join tx_ReviewRemediationBids_Environmental_Environmental remediation_bids
on remediation_bids.property_id = ET.Property_ID
and ET.id = remediation_bids.id
and ET.ods_deleted_flag = 'N'


left join tx_ReviewTestingBids_Environmental_Environmental testing_bids
on testing_bids.property_id = ET.Property_ID
and ET.id = testing_bids.id
and ET.ods_deleted_flag = 'N'

left join tx_ReviewTestingResults_Environmental_Environmental review_test
on review_test.property_id = ET.Property_ID
and ET.id = review_test.id
and ET.ods_deleted_flag = 'N'

left join tx_ThirdPartyClearance_Environmental_Environmental third_p_c
on third_p_c.property_id = ET.Property_ID
and ET.id = third_p_c.id
and ET.ods_deleted_flag = 'N'

left join dbo.ODS_Lender_Vendor_Role_Person_XREF lvrpx with(nolock)
  /*Equator works on ppl. Ppl role up to company. Ppl have roles.*/
  on  lvrpx.Person_ID  =   ET.Target_PID
  AND lvrpx.lender_id  =   1617 /* Fannie Mae Workstation */
  AND lvrpx.ODS_Deleted_Flag = 'N'

left join dbo.ODS_Vendor vendor_name with(nolock)
  on vendor_name.Vendor_ID = lvrpx.Vendor_ID
  and vendor_name.ODS_Deleted_Flag = 'N'

inner join REO_DATE_FILTER EO
  on EO.Property_ID = ET.Property_ID
  and eo.event_type_ID = ET.event_type_ID

LEFT JOIN dbo.ODS_Property_Role_Person SMGR WITH(NOLOCK)
  ON ET.Property_ID = SMGR.Property_ID
  AND SMGR.ODS_Deleted_Flag = 'N'
  AND SMGR.Lender_ID = 1617
  AND SMGR.Role_ID = 1269 /*Sales Mgr*/

LEFT JOIN dbo.ODS_Person SMGR_NAME WITH(NOLOCK)
  ON SMGR.Person_ID = SMGR_NAME.Person_ID
  AND SMGR_NAME.ODS_Deleted_Flag = 'N'

LEFT JOIN dbo.ODS_Event_Lender EventLender WITH(NOLOCK)
  ON  ET.event_lender_id = EventLender.id
  and EventLender.Lender_ID =  Property.Lender_ID
  and EventLender.ods_deleted_flag = 'N'
  
LEFT JOIN dbo.ODS_Event Event WITH(NOLOCK)
  ON EventLender.event_id = Event.id
  and EventLender.Lender_ID = property.Lender_ID
  AND Event.ODS_Deleted_Flag = 'N'  

where upper(lk_up_Issue_type.type_group) = 'ENVIRONMENTAL_ISSUE'
and ET.Event_Lender_ID not in (194759,239747,210210,210211,193433,184719
,142841,142809,184718,142808,210213,184713,184851,186650,186649,186657,186656,184853)
and ET.ODS_Deleted_Flag = 'N'
and ET.task_status in (1,2,3,4,5)
/*and (Et.close_date >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0) OR Et.close_date IS NULL)*/
);
quit;

data environmental_repair_other;
set environmental_repair;
WHERE TASK_OPEN_DT >= EO_OPEN_DT AND DATE_FILTER <= EO_CLOSE_DT AND TASK_CAT ^= 'NA';
run;

data environmental_repair_CE;
set environmental_repair;
WHERE TASK_OPEN_DT = EO_OPEN_DT AND DATE_FILTER = EO_CLOSE_DT AND (Event_Lender_ID = 184705);
run;

data environmental_repair_combined;
set environmental_repair_CE environmental_repair_other;
run;


data environmental_repair1;
length recomm_remediation1  recomm_remediation2 recomm_remediation3 
  recomm_testing1 recomm_testing2 recomm_testing3 recomm_review_test 
    recomm_thirdp_cl $30.;
set environmental_repair_combined;
run;

DATA OUTPUT.environmental_repair_&CM.;
SET environmental_repair1;
report_dt=today();
format report_dt date9.;
RUN;

DATA environmental_repair2;
SET output.environmental_repair_20:;
report_first_day=intnx('month',report_dt,0,'B');
format report_first_day date9.;
format TASK_STATUS $11.;
informat TASK_STATUS $11.;
RUN;

/*
proc datasets library = oasisbox nolist;
delete environmental_repair;
quit;

data oasisbox.environmental_repair;
set environmental_repair2;
run;

proc sql;
 Connect To oracle AS TEST (&oasis_box);
 EXECUTE(grant select on environmental_repair to CMAR_READ_RL) BY TEST;
QUIT;
*/

data environmental_repair2;
   retain 
	RECOMM_REMEDIATION1
	RECOMM_REMEDIATION2
	RECOMM_REMEDIATION3
	RECOMM_TESTING1
	RECOMM_TESTING2
	RECOMM_TESTING3
	RECOMM_REVIEW_TEST
	RECOMM_THIRDP_CL
	ID
	EVENT_LENDER_ID
	REO_ID
	FNMN_LN_ID
	ADDRESS
	CITY
	STATE
	COUNTY
	STATUS
	TARGET_PID
	TASK_OPEN_DT
	EO_OPEN_DT
	TASK_CLOSE_DT
	EO_CLOSE_DT
	DATE_FILTER
	DAYS_OPEN
	PROPERTY_ID
	TASK_STATUS
	TASK_STATUS_N
	CLOSED_TASK_STATUS
	EVENT_TYPE
	DAYS_OVERDUE
	ASSIGNED_TO
	SALES_REP
	CONSULTANT_ID
	VENDOR_NAME
	TASK_CAT
	EVENT_NAME
	REPORT_DT
	REPORT_FIRST_DAY
	OPEN_DATE_DT
	SALES_MGR
	EVENT_TYPE_ID
;
   set environmental_repair2;
run;

proc sql;
   connect to oracle as OASIS (&Oasis);
   execute(execute OASISPRD.pr_truncate_schema_table('OASISPRD','environmental_repair')) by OASIS;
   disconnect from OASIS;
quit;
   
proc sql;
   insert into OASISPRD.environmental_repair(sasdatefmt=(REPORT_DT='date9.' REPORT_FIRST_DAY='date9.' TASK_OPEN_DT='DATE9.'
   TASK_CLOSE_DT='DATE9.' EO_OPEN_DT='DATE9.' EO_CLOSE_DT='DATE9.' DATE_FILTER='DATE9.' OPEN_DATE_DT='DATETIME22.3.'))
   select * from environmental_repair2;
quit;

data environmental_repair3;
set environmental_repair1;
if TASK_STATUS_N ^= 5;
run;

proc sql;
CREATE TABLE Complete_Environmental_Order AS
SELECT
    REO_ID,
    EVENT_TYPE,
    TASK_OPEN_DT,
  	open_date_dt,
    TASK_CLOSE_DT format date9.,
    STATE,
    COUNTY,
    CITY,
    ADDRESS,
    sales_rep,
  	Sales_MGR,
    DAYS_OPEN AS TASK_OPEN_DAYS,
    EVENT_NAME AS TASK_CAT,
    CASE 
    WHEN TASK_CLOSE_DT IS NULL THEN 'Open'
    ELSE 'Complete' END AS EVENT_STATUS
FROM
    work.environmental_repair3
WHERE
    EVENT_NAME = 'Complete Environmental Order';
QUIT;

data output.Complete_Envir_Order_&cm.;
set Complete_Environmental_Order;
  if EVENT_STATUS = 'Open' then OPEN_INDICATOR = 1;
  else OPEN_INDICATOR = 0;
  report_dt=today();
  format report_dt date9.;
run;

PROC SQL;
CREATE TABLE EVENT_TYPE AS
SELECT 
  DISTINCT EVENT_TYPE
FROM 
  environmental_repair3;
QUIT;

data _null_;
    set EVENT_TYPE end=eof;
    if eof then call symputx('EVENT_TYPE_CNT',_n_,'g');    
run;

proc sql;
connect to netezza(&ntzcmap.);
create table DAYS_UTM as
select *
from connection to netezza (
select
	REO_ID, 
	DAYS_IN_UTM_STATUS
from
	(
	select 
		REO_ID, 
		DAYS_IN_UTM_STATUS,
		RANK() OVER (PARTITION BY reo_id ORDER BY ACTIVITY_MONTH DESC) as rank1 
	from 
		RE_COMBINED_DATA
	order by 
		reo_id
	)a
where
	rank1 = 1
) order by REO_ID 
;
disconnect from netezza;
quit;

/*
data _null_;
call symput('EVENT_TYPE','Discoloration');
run;
*/
/****************************************************/
/* run Macro to calculate all event type separately */
/****************************************************/

%macro compute(EVENT_TYPE_CNT);

%do i=1 %to &EVENT_TYPE_CNT.;

data _null_;
  set EVENT_TYPE;
  if _n_= &i. then do;
  call symput('EVENT_TYPE',strip(EVENT_TYPE));
  end;
run;

/*********************************************************************/
/********************* Testing bids caculation ***********************/
/*********************************************************************/

PROC SQL;
connect to oracle (&oasis);
CREATE TABLE Testing_bids_ID AS

SELECT
  reo_id
  , event_name
  , task_cat
  , task_open_dt
  , open_date_dt 
  , task_close_dt 
  , days_open
  , recomm_testing1
  , recomm_testing2
  , rank1
  , event_status
  , event_type
from Connection To Oracle
(
    SELECT 
        b.*,
        CASE
            WHEN rank1 = 1 and event_name = 'Review Testing Bids' and 
      ((recomm_testing1 = 'Recommend' or recomm_testing2 = 'Recommend') and task_close_dt is not null) then 'Complete' 
            WHEN event_name = 'Review Testing Bids' and task_close_dt is NULL then 'Open' end as event_status
    from
    (
        SELECT
            a.reo_id
            , a.event_name
            , a.task_cat
            , a.task_open_dt
      		, a.open_date_dt
            , a.task_close_dt
            , a.days_open
            , a.recomm_testing1
            , a.recomm_testing2
      		, a.event_type
            , RANK() OVER (PARTITION BY reo_id, task_cat ORDER BY open_date_dt DESC) as rank1 
        from 
            OASISPRD.Environmental_repair a
        where 
            event_name = 'Review Testing Bids' and event_type = %unquote(%str(%')&event_type.%str(%')) and report_first_day = TRUNC(sysdate, 'MONTH')
        order by 
            task_cat, reo_id, task_open_dt
    )b
    where task_cat = 'Testing_bids' and rank1 = 1
)C
where event_status is not null;
QUIT;

PROC SQL;
CREATE TABLE TESTING_BIDS_MIN_DT AS
SELECT 
  REO_ID,
  EVENT_TYPE,
  STATE,
  COUNTY,
  CITY,
  ADDRESS,
  Sales_Rep,
  Sales_MGR,
  MIN(TASK_OPEN_DT) FORMAT DATE9. AS MIN_OPEN_DT 
FROM
  WORK.environmental_repair3
WHERE
  TASK_CAT = 'Testing_bids' AND event_type = "&event_type."
GROUP BY
  REO_ID, EVENT_TYPE, STATE, COUNTY, CITY, ADDRESS, Sales_Rep, Sales_MGR
ORDER BY 
  REO_ID;
QUIT;

PROC SORT DATA=TESTING_BIDS_MIN_DT; BY REO_ID; RUN;
PROC SORT DATA=Testing_bids_ID; BY REO_ID; RUN;

DATA Testing_bids;
MERGE TESTING_BIDS_MIN_DT(in=a) Testing_bids_ID(in=b);
by reo_id;
if a;
IF EVENT_STATUS = '' THEN EVENT_STATUS = 'Open';
run;

PROC SQL;
CREATE TABLE Testing_bids2 AS
SELECT
  A.*,
  CASE
  WHEN EVENT_STATUS = 'Open' THEN today() - MIN_OPEN_DT
  ELSE datepart(TASK_CLOSE_DT) - MIN_OPEN_DT
  END AS TASK_OPEN_DAYS
FROM 
  WORK.Testing_bids A;
QUIT;

DATA Testing_bids_revis;
set environmental_repair3;
if event_name = 'Review Testing Bids' AND TASK_CLOSE_DT ^= '' then Review_Testing_Bids = 1;
ELSE Review_Testing_Bids=0;

if recomm_testing1 = 'Need Revisions' or recomm_testing2 = 'Need Revisions' then Need_Revisions = 1;
ELSE Need_Revisions=0;

if TASK_CAT = 'Testing_bids' AND event_type = "&event_type.";
RUN;
      
PROC MEANS data=Testing_bids_revis noprint;
class reo_id;
var Review_Testing_Bids Need_Revisions;
types  reo_id; 
output out=Testing_bids_revis1(drop=_type_ _freq_ _stat_) SUM(Review_Testing_Bids Need_Revisions)= ;
RUN;

PROC SORT data=Testing_bids_revis1; by reo_id; RUN;
PROC SORT data=Testing_bids2; by reo_id; RUN;

data Testing_bids3 (keep=REO_ID MIN_OPEN_DT TASK_CAT TASK_CLOSE_DT EVENT_STATUS TASK_OPEN_DAYS 
          Review_Testing_Bids Need_Revisions event_type STATE COUNTY CITY ADDRESS Sales_Rep Sales_MGR);

merge Testing_bids2(in=a) Testing_bids_revis1(in=b);
if a;
by reo_id;
IF TASK_CAT = '' THEN TASK_CAT = 'Testing_bids';
run;

proc sort data=Testing_bids3; by reo_id; run;

data Testing_bids3;
merge Testing_bids3 (in=a) DAYS_UTM (in=b);
by reo_id;
if a;
run;

DATA Testing_bids_final_&i.;
set Testing_bids3;
RUN;


/*********************************************************************/
/********************* Testing part caculation ***********************/
/*********************************************************************/

PROC SQL;
connect to oracle (&oasis);
CREATE TABLE Testing_ID AS

SELECT
reo_id
, event_name
, task_cat
, task_open_dt
, open_date_dt
, task_close_dt 
, RECOMM_REVIEW_TEST
, rank1
, event_status
, event_type
from Connection To Oracle
(
    SELECT 
        b.*,
        CASE
            WHEN rank1 = 1 and event_name = 'Review Testing Results' and 
      ((RECOMM_REVIEW_TEST = 'Recommend Remediation' or RECOMM_REVIEW_TEST = 'Clear to Market') and task_close_dt is not null) then 'Complete' 
            WHEN event_name = 'Review Testing Results' and task_close_dt is NULL then 'Open' end as event_status
    from
    (
        SELECT
            a.reo_id
            , a.event_name
            , a.task_cat
            , a.task_open_dt
      , a.open_date_dt
            , a.task_close_dt
            , a.days_open
      , a.event_type
      , RECOMM_REVIEW_TEST
            , RANK() OVER (PARTITION BY reo_id, task_cat ORDER BY open_date_dt DESC) as rank1 
        from 
            OASISPRD.Environmental_repair a
        where 
            event_name = 'Review Testing Results' and event_type = %unquote(%str(%')&event_type.%str(%')) and report_first_day = TRUNC(sysdate, 'MONTH')
        order by 
            task_cat, reo_id, task_open_dt
    )b
    where task_cat = 'Testing' and rank1 = 1
)C
where event_status is not null;
QUIT;

PROC SQL;
CREATE TABLE TESTING_MIN_DT AS
SELECT 
  REO_ID,
  EVENT_TYPE,
  STATE,
  COUNTY,
  CITY,
  ADDRESS,
  Sales_Rep,
  Sales_MGR,
  MIN(TASK_OPEN_DT) FORMAT DATE9. AS MIN_OPEN_DT
FROM
  WORK.environmental_repair3
WHERE
  TASK_CAT = 'Testing' and event_type = "&event_type."
GROUP BY
  REO_ID, EVENT_TYPE, STATE, COUNTY, CITY, ADDRESS, Sales_Rep, Sales_MGR
ORDER BY 
  REO_ID;
QUIT;

PROC SORT DATA=TESTING_MIN_DT; BY REO_ID; RUN;
PROC SORT DATA=Testing_ID; BY REO_ID; RUN;

DATA Testing;
MERGE TESTING_MIN_DT(in=a) Testing_ID(in=b);
by reo_id;
if a;
IF EVENT_STATUS = '' THEN EVENT_STATUS = 'Open';
run;

PROC SQL;
CREATE TABLE Testing1 AS
SELECT
  A.*,
  CASE
  WHEN EVENT_STATUS = 'Open' THEN today() - MIN_OPEN_DT
  ELSE datepart(TASK_CLOSE_DT) - MIN_OPEN_DT
  END AS TASK_OPEN_DAYS
FROM 
  WORK.Testing A;
QUIT;

PROC FREQ data= environmental_repair3 noprint;
WHERE task_cat = 'Testing' and event_type = "&event_type.";
TABLE reo_id*recomm_review_test / out=Testing_rev_cl_rec;
RUN;

PROC SORT data=Testing_rev_cl_rec; by reo_id; run;

PROC TRANSPOSE data=Testing_rev_cl_rec out=Testing_rev_cl_rec1(drop=_name_ _label_);
    by reo_id;
    id recomm_review_test;
    var count;
RUN;

DATA Testing_rev_cl_rec2;
set Testing_rev_cl_rec1;
if Need_Revisions_on_Test =. THEN Need_Revisions_on_Test = 0;
if Recommend_Remediation =. THEN Recommend_Remediation = 0;
if Clear_To_Market =. THEN Clear_To_Market = 0;
RUN;

PROC FREQ data= environmental_repair3 noprint;
where task_cat = 'Testing' AND Event_Name = 'Review Testing Results' AND EVENT_TYPE = "&event_type." AND TASK_CLOSE_DT IS NOT NULL;
table reo_id*EVENT_NAME / out=Testing_REV_TEST_RES(rename=(COUNT=Review_Testing_Results) DROP=PERCENT EVENT_NAME);
run;

PROC SORT data=Testing1; BY REO_ID; RUN;
PROC SORT data=Testing_rev_cl_rec2; BY REO_ID; RUN;
PROC SORT data=Testing_REV_TEST_RES; BY REO_ID; RUN;

DATA Testing2;
MERGE Testing1(IN=A) Testing_rev_cl_rec2(IN=B) Testing_REV_TEST_RES(IN=C);
BY REO_ID;
task_cat='Testing';
if Review_Testing_Results =. then Review_Testing_Results=0;
IF A;
RUN;

proc sort data=Testing2; by reo_id; run;

data Testing2;
merge Testing2 (in=a) DAYS_UTM (in=b);
by reo_id;
if a;
run;

DATA Testing_Final_&i.;
set Testing2(drop=event_name task_open_dt recomm_review_test rank1);
run;

/*********************************************************************/
/****************** Remediation Bids part caculation *****************/
/*********************************************************************/

PROC SQL;
connect to oracle (&oasis);
CREATE TABLE Remediation_bids_ID AS

SELECT
reo_id
, event_name
, task_cat
, task_open_dt
, open_date_dt 
, task_close_dt 
, days_open
, recomm_remediation1
, recomm_remediation2
, rank1
, event_status
, event_type
from Connection To Oracle
(
    SELECT 
        b.*,
        CASE
            WHEN rank1 = 1 and event_name = 'Review Remediation Bids' and 
      ((recomm_remediation1 = 'Recommend' or recomm_remediation2 = 'Recommend') and task_close_dt is not null) then 'Complete' 
            WHEN event_name = 'Review Remediation Bids' and task_close_dt is NULL then 'Open' end as event_status
    from
    (
        SELECT
            a.reo_id
            , a.event_name
            , a.task_cat
            , a.task_open_dt
      , a.open_date_dt
            , a.task_close_dt
            , a.days_open
            , a.recomm_remediation1
            , a.recomm_remediation2
      , a.event_type
            , RANK() OVER (PARTITION BY reo_id, task_cat ORDER BY open_date_dt DESC) as rank1 
        from 
            OASISPRD.Environmental_repair a
        where 
            event_name = 'Review Remediation Bids' and event_type = %unquote(%str(%')&event_type.%str(%')) and report_first_day = TRUNC(sysdate, 'MONTH')
        order by 
            task_cat, reo_id, task_open_dt
    )b
    where task_cat = 'Remediation_bids' and rank1 = 1
)C
where event_status is not null;
QUIT;

PROC SQL;
CREATE TABLE Remediation_BIDS_MIN_DT AS
SELECT 
  REO_ID,
  EVENT_TYPE,
  STATE,
  COUNTY,
  CITY,
  ADDRESS,
  Sales_Rep,
  Sales_MGR,
  MIN(TASK_OPEN_DT) FORMAT DATE9. AS MIN_OPEN_DT 
FROM
  WORK.environmental_repair3
WHERE
  TASK_CAT = 'Remediation_bids' and event_type = "&event_type."
GROUP BY
  REO_ID, EVENT_TYPE, STATE, COUNTY, CITY, ADDRESS, Sales_Rep, Sales_MGR
ORDER BY 
  REO_ID;
QUIT;

PROC SORT DATA=Remediation_BIDS_MIN_DT; BY REO_ID; RUN;
PROC SORT DATA=Remediation_bids_ID; BY REO_ID; RUN;

DATA Remediation_bids;
MERGE Remediation_BIDS_MIN_DT(in=a) Remediation_bids_ID(in=b);
by reo_id;
if a;
IF EVENT_STATUS = '' THEN EVENT_STATUS = 'Open';
run;

PROC SQL;
CREATE TABLE Remediation_bids2 AS
SELECT
  A.*,
  CASE
  WHEN EVENT_STATUS = 'Open' THEN today() - MIN_OPEN_DT
  ELSE datepart(TASK_CLOSE_DT) - MIN_OPEN_DT
  END AS TASK_OPEN_DAYS
FROM 
  WORK.Remediation_bids A;
QUIT;

DATA Remediation_bids_revis;
set environmental_repair3;
if event_name = 'Review Remediation Bids' AND TASK_CLOSE_DT ^= '' then Review_Remediation_Bids = 1;
ELSE Review_Remediation_Bids=0;

if recomm_remediation1 = 'Need Revisions' or recomm_remediation2 = 'Need Revisions' then Need_Revisions = 1;
ELSE Need_Revisions=0;

IF recomm_remediation1 = 'Recommend' or recomm_remediation2 = 'Recommend' then Recommend = 1;
ELSE Recommend = 0;

if TASK_CAT = 'Remediation_bids' and event_type = "&event_type.";
RUN;

PROC MEANS data=Remediation_bids_revis noprint;
class reo_id;
var Review_Remediation_Bids Need_Revisions Recommend;
types  reo_id; 
output out=Remediation_bids_revis1(drop=_type_ _freq_ _stat_) SUM(Review_Remediation_Bids Need_Revisions Recommend)= ;
RUN;

PROC SORT data=Remediation_bids_revis1; by reo_id; RUN;
PROC SORT data=Remediation_bids2; by reo_id; RUN;

data Remediation_bids3 (keep=REO_ID MIN_OPEN_DT TASK_CAT TASK_CLOSE_DT EVENT_STATUS TASK_OPEN_DAYS 
            Review_Remediation_Bids Need_Revisions Recommend event_type STATE COUNTY CITY ADDRESS Sales_Rep Sales_MGR);
merge Remediation_bids2(in=a) Remediation_bids_revis1(in=b);
if a;
by reo_id;
IF TASK_CAT = '' THEN TASK_CAT = 'Remediation_bids';
run;

proc sort data=Remediation_bids3; by reo_id; run;

data Remediation_bids3;
merge Remediation_bids3 (in=a) DAYS_UTM (in=b);
by reo_id;
if a;
run;

DATA Remediation_bids_Final_&i.;
set Remediation_bids3;
run;

/*********************************************************************/
/******************** Remediation part caculation ********************/
/*********************************************************************/

PROC SQL;
connect to oracle (&oasis);
CREATE TABLE Remediation_ID AS

SELECT
reo_id
, event_name
, task_cat
, task_open_dt
, open_date_dt
, task_close_dt 
, RECOMM_THIRDP_CL
, rank1
, event_status
, event_type
from Connection To Oracle
(
    SELECT 
        b.*,
        CASE
            WHEN rank1 = 1 and event_name = 'Third Party Clearance' and 
      (RECOMM_THIRDP_CL = 'Clear to Market' and task_close_dt is not null) then 'Complete' 
            WHEN event_name = 'Third Party Clearance' and RECOMM_THIRDP_CL != 'Clear to Market' then 'Open' end as event_status
    from
    (
        SELECT
            a.reo_id
            , a.event_name
            , a.task_cat
            , a.task_open_dt
      , a.open_date_dt
            , a.task_close_dt
            , a.days_open
      , a.event_type
          , RECOMM_THIRDP_CL
            , RANK() OVER (PARTITION BY reo_id, task_cat ORDER BY open_date_dt DESC) as rank1 
        from 
            OASISPRD.Environmental_repair a
        where 
            event_name = 'Third Party Clearance' and event_type = %unquote(%str(%')&event_type.%str(%')) and report_first_day = TRUNC(sysdate, 'MONTH')
        order by 
            task_cat, reo_id, task_open_dt
    )b
    where task_cat = 'Remediation' and rank1 = 1
)C
where event_status is not null;
QUIT;

PROC SQL;
CREATE TABLE Remediation_MIN_DT AS
SELECT 
  REO_ID,
  EVENT_TYPE,
  STATE,
  COUNTY,
  CITY,
  ADDRESS,
  Sales_Rep,
  Sales_MGR,
  MIN(TASK_OPEN_DT) FORMAT DATE9. AS MIN_OPEN_DT 
FROM
  WORK.environmental_repair3
WHERE
  TASK_CAT = 'Remediation' and event_type = "&event_type."
GROUP BY
  REO_ID, EVENT_TYPE, STATE, COUNTY, CITY, ADDRESS, Sales_Rep, Sales_MGR
ORDER BY 
  REO_ID;
QUIT;

PROC SORT DATA=Remediation_MIN_DT; BY REO_ID; RUN;
PROC SORT DATA=Remediation_ID; BY REO_ID; RUN;

DATA Remediation;
MERGE Remediation_MIN_DT(in=a) Remediation_ID(in=b);
by reo_id;
if a;
IF EVENT_STATUS = '' THEN EVENT_STATUS = 'Open';
run;

PROC SQL;
CREATE TABLE Remediation1 AS
SELECT
  A.*,
  CASE
  WHEN EVENT_STATUS = 'Open' THEN today() - MIN_OPEN_DT
  ELSE datepart(TASK_CLOSE_DT) - MIN_OPEN_DT
  END AS TASK_OPEN_DAYS
FROM 
  WORK.Remediation A;
QUIT;

PROC FREQ data= environmental_repair3 noprint;
WHERE task_cat = 'Remediation' and event_type = "&event_type.";
TABLE reo_id*RECOMM_THIRDP_CL / out=Remediation_rev_rec;
RUN;

PROC SORT data=Remediation_rev_rec; by reo_id; run;

PROC TRANSPOSE data=Remediation_rev_rec out=Remediation_rev_rec1(drop=_name_ _label_);
    by reo_id;
    id RECOMM_THIRDP_CL;
    var count;
RUN;

DATA Remediation_rev_rec2;
set Remediation_rev_rec1;
if No_Clearance =. THEN No_Clearance = 0;
if Clear_to_Market =. THEN Clear_to_Market = 0;
RUN;

PROC FREQ data= environmental_repair3 noprint;
where task_cat = 'Remediation' AND Event_Name = 'Third Party Clearance' and event_type = "&event_type." AND TASK_CLOSE_DT IS NOT NULL;
table reo_id*EVENT_NAME / out=Remediation_ThirdP_CL(rename=(COUNT=Third_Party_Clearance) DROP=PERCENT EVENT_NAME);
run;

PROC SORT data=Remediation1; BY REO_ID; RUN;
PROC SORT data=Remediation_rev_rec2; BY REO_ID; RUN;
PROC SORT data=Remediation_ThirdP_CL; BY REO_ID; RUN;

DATA Remediation2;
MERGE Remediation1(IN=A) Remediation_rev_rec2(IN=B) Remediation_ThirdP_CL(IN=C);
BY REO_ID;
task_cat='Remediation';
if Third_Party_Clearance =. then Third_Party_Clearance = 0;
IF A;
RUN;

proc sort data=Remediation2; by reo_id; run;

data Remediation2;
merge Remediation2 (in=a) DAYS_UTM (in=b);
by reo_id;
if a;
run;

DATA Remediation_Final_&i.;
set Remediation2(drop=event_name task_open_dt RECOMM_THIRDP_CL rank1);
run;

/* Merge all Tables together */

%end;
%mend;
%compute(EVENT_TYPE_CNT=&EVENT_TYPE_CNT.);

data output.Testing_bids_&cm. (drop=task_close_dt rename=(date=task_close_dt));
  set Testing_bids_final_1 - Testing_bids_final_&EVENT_TYPE_CNT.;
  report_dt=today();
  format report_dt date9.;
  date=datepart(task_close_dt);
  format date date9.;
run;

data output.Testing_&cm. (drop=task_close_dt rename=(date=task_close_dt));
  set Testing_final_1 - Testing_final_&EVENT_TYPE_CNT.;
  report_dt=today();
  format report_dt date9.;
  date=datepart(task_close_dt);
  format date date9.;
run;

data output.Remediation_bids_&cm. (drop=task_close_dt rename=(date=task_close_dt));
  set Remediation_bids_final_1 - Remediation_bids_final_&EVENT_TYPE_CNT.;
  report_dt=today();
  format report_dt date9.;
  date=datepart(task_close_dt);
  format date date9.;
run;

data output.Remediation_&cm. (drop=task_close_dt rename=(date=task_close_dt));
  set Remediation_final_1 - Remediation_final_&EVENT_TYPE_CNT.;
  report_dt=today();
  format report_dt date9.;
  date=datepart(task_close_dt);
  format date date9.;
run;

data Testing_bids;
  set output.Testing_bids_20:;
  report_first_day=intnx('month',report_dt,0,'B');
  format report_first_day date9.;
run;

data Testing;
set output.Testing_20:;
report_first_day=intnx('month',report_dt,0,'B');
format report_first_day date9.;
run;

data Remediation_bids;
set output.Remediation_bids_20:;
report_first_day=intnx('month',report_dt,0,'B');
format report_first_day date9.;
run;

data Remediation;
set output.Remediation_20:;
report_first_day=intnx('month',report_dt,0,'B');
format report_first_day date9.;
run;

DATA Complete_Envir_Order;
SET output.Complete_Envir_Order_20:;
report_first_day=intnx('month',report_dt,0,'B');
format report_first_day date9.;
IF TASK_OPEN_DT >= report_first_day then INFLOW = 1;
ELSE INFLOW = 0;
RUN;

data Environmental_repair_final;
set Complete_Envir_Order
    Testing_bids
    Testing
    Remediation_bids
    Remediation;
run;

PROC SQL;
connect to oracle (&oasis);
CREATE TABLE CONSULTANT AS
SELECT
  *
from Connection To Oracle
(
  SELECT 
    DISTINCT reo_id
    ,VENDOR_NAME
  FROM
    OASISPRD.environmental_repair
  WHERE
    VENDOR_NAME IS NOT NULL
);
quit;

PROC SORT DATA=Environmental_repair_final; BY REO_ID; RUN;
PROC SORT DATA=CONSULTANT; BY REO_ID; RUN;

DATA Environmental_repair_final1;
MERGE Environmental_repair_final (IN=A) CONSULTANT(IN=B);
BY REO_ID;
IF A;
RUN;

data COMPLETE_ENVIRONMENTAL (keep=reo_id SKIPPED_CANCELLED OPEN_INDICATOR report_first_day);
set Environmental_repair_final1;
IF TASK_CAT = 'Complete Environmental Order';
SKIPPED_CANCELLED = 0;
RUN;

PROC SORT DATA=COMPLETE_ENVIRONMENTAL NODUPKEY; BY REO_ID report_first_day; RUN;
PROC SORT DATA=Environmental_repair_final1; BY REO_ID report_first_day; RUN;

DATA Environmental_repair_final2;
MERGE Environmental_repair_final1(IN=A drop=OPEN_INDICATOR) COMPLETE_ENVIRONMENTAL(IN=B);
BY REO_ID report_first_day;
IF A;
IF SKIPPED_CANCELLED =. THEN SKIPPED_CANCELLED = 1;
RUN;

/*
proc datasets library = oasisbox nolist;
delete environmental_repair_final;
quit;

data oasisbox.Environmental_repair_final;
set Environmental_repair_final2;
run;

proc sql;
 Connect To oracle AS TEST (&oasis_box);
 EXECUTE(grant select on Environmental_repair_final to CMAR_READ_RL) BY TEST;
QUIT;
*/

data Environmental_repair_final2;
   retain 
	REO_ID
	EVENT_TYPE
	TASK_OPEN_DT
	TASK_CLOSE_DT
	STATE
	SALES_REP
	TASK_OPEN_DAYS
	TASK_CAT
	EVENT_STATUS
	REPORT_DT
	REPORT_FIRST_DAY
	INFLOW
	MIN_OPEN_DT
	REVIEW_TESTING_BIDS
	NEED_REVISIONS
	RECOMMEND_REMEDIATION
	CLEAR_TO_MARKET
	NEED_REVISIONS_ON_TEST
	REVIEW_TESTING_RESULTS
	REVIEW_REMEDIATION_BIDS
	RECOMMEND
	NO_CLEARANCE
	THIRD_PARTY_CLEARANCE
	VENDOR_NAME
	SKIPPED_CANCELLED
	OPEN_DATE_DT
	COUNTY
	CITY
	ADDRESS
	SALES_MGR
	OPEN_INDICATOR
	DAYS_IN_UTM_STATUS
;
   set Environmental_repair_final2;
run;

proc sql;
   connect to oracle as OASIS (&Oasis);
   execute(execute OASISPRD.pr_truncate_schema_table('OASISPRD','Environmental_repair_final')) by OASIS;
   disconnect from OASIS;
quit;
   
proc sql;
   insert into OASISPRD.Environmental_repair_final(sasdatefmt=(REPORT_DT='date9.' REPORT_FIRST_DAY='date9.' TASK_OPEN_DT='DATE9.'
   TASK_CLOSE_DT='DATE9.' MIN_OPEN_DT='DATE9.' OPEN_DATE_DT='DATETIME22.3.'))
   select * from Environmental_repair_final2;
quit;
