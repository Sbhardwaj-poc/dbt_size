CREATE OR REPLACE PROCEDURE DEPRECATED.TRANSFORM."KPIs|2022|New Members Deposit Penetration$2327$procedure"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'begin
    -- ETL Name: KPIs|2022|New Members Deposit Penetration --
    -- ETL Link: https://uccu.domo.com/datacenter/dataflows/2327/details#history --
    
    -- Tile Name: DNA|All Unique Pers/Org Numbers --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/f8ee657d-de2d-46f1-8de9-b0334460a10f/details/overview 
    -- 
    create or replace temp table 
    temp."565f00d4-554d-4a49-895a-1ceda68d07aa"
    as(
    select * 
    -- from raw."DNA|All Unique Pers/Org Numbers$f8ee657d-de2d-46f1-8de9-b0334460a10f"
    -- from webforms."DNA|All Unique Pers/Org Numbers$f8ee657d-de2d-46f1-8de9-b0334460a10f"
    from transform."DNA|All Unique Pers/Org Numbers$f8ee657d-de2d-46f1-8de9-b0334460a10f"
    );
    

    -- Tile Name: Persons --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."00a17358-0903-4f15-8267-fad2a453b5bd"
    as(
    select 
    * 
    from 
    temp."565f00d4-554d-4a49-895a-1ceda68d07aa"
    where
    ("Is Admin" = ''N'' 
    AND "Is Branch" = ''N'' 
    AND "Test/Internal" = ''N'' 
    AND "PURGEYN" = ''N'' 
    AND STARTSWITH("Unique Pers/Org Number",''P'') 
    AND "Original Membership Date" IS NOT NULL)
    );
    

    -- Tile Name: Member Y/N|Current Method|Month End --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/4067d79f-64a9-48ca-9df3-f04ab0375452/details/overview 
    -- 
    create or replace temp table 
    temp."0de9950b-1dc4-4a0c-9c53-83e4bb7540ba"
    as(
    select * 
    -- from raw."Member Y/N|Current Method|Month End$4067d79f-64a9-48ca-9df3-f04ab0375452"
    -- from webforms."Member Y/N|Current Method|Month End$4067d79f-64a9-48ca-9df3-f04ab0375452"
    from transform."Member Y/N|Current Method|Month End$4067d79f-64a9-48ca-9df3-f04ab0375452"
    );
    

    -- Tile Name: Member Y/N Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."d1af3a39-b4f4-4525-919b-72333dbf9ab0"
    as(
    select 
    "EFFDATE"
,"Unique Pers/Org Number"
,"Member Y/N Change"
    from 
    temp."0de9950b-1dc4-4a0c-9c53-83e4bb7540ba"
    );
    

    -- Tile Name: Executive Management|KPIs|2022|Active Checking|Trend --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/cdd69592-5a2a-436d-88fa-bffd64c01b4c/details/overview 
    -- 
-- OMIT ROW SETTING -- 
-- 
    create or replace temp table 
    temp."83637d9b-aaa1-473f-aa2b-45ca60cbddbb"
    as(
    select * 
replace (
cast("Age" as number) as "Age"
)
    -- from raw."OKR|2022|Active Spend Account|AllTime$cdd69592-5a2a-436d-88fa-bffd64c01b4c"
    -- from webforms."OKR|2022|Active Spend Account|AllTime$cdd69592-5a2a-436d-88fa-bffd64c01b4c"
    from transform."OKR|2022|Active Spend Account|AllTime$cdd69592-5a2a-436d-88fa-bffd64c01b4c"
    );
    

    -- Tile Name: Direct Deposit --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."68a726a6-a79d-48b8-aabd-e83dc12a7ddb"
    as(
    select 
    "EFFDATE"
,"Unique Pers/Org Number"
,"INCOMELEVCD"
,"INCOMELEVLOW"
,"INCOMELEVHIGH"
,"Generation (Detailed)"
,"Generation"
,"Age"
,"Membership Tenure"
,"Membership Tenure (Months)"
,"Membership Tenure (Days)"
,("Has Open Checking") as "Checking Member"
,("Has Debit Swipe") as "Debit Swipe Member"
,"ACH Deposit Count"
,"ACH Deposit Amount"
,"Debit Swipe Count"
,"Debit Swipe Amount"
,"Direct Deposit Member"
,"Deposit Member"
,"Member Y/N"
,"Joint Member Y/N"
,"Cash Deposit Amount"
,"Check Deposit Amount"
    from 
    temp."83637d9b-aaa1-473f-aa2b-45ca60cbddbb"
    );
    

    -- Tile Name: Join Member Y/N Change --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."59d57ff3-035e-4de6-bc38-ae011ed888dd"
    as(
    select t1.* 
    ,t2.* 
exclude (
"EFFDATE"
,"Unique Pers/Org Number"
)
    from 
    temp."68a726a6-a79d-48b8-aabd-e83dc12a7ddb" as t1
    left outer join
    temp."d1af3a39-b4f4-4525-919b-72333dbf9ab0" as t2
    on 
    t1."EFFDATE" = t2."EFFDATE"
and t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: 2 Month Member --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."362151f1-e3d9-44cd-8552-d54709d70ccb"
    as(
    select 
    * 
    from 
    temp."59d57ff3-035e-4de6-bc38-ae011ed888dd"
    where
    ("Membership Tenure (Months)" = 2)
    );
    

    -- Tile Name: Member Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."229db9c2-1d92-45f0-8233-93c774e85fe1"
    as(
    select 
    "Unique Pers/Org Number"
,"Original Membership Account"
,"Original Membership Date"
,"Original Membership Type"
,"UVU Membership Type"
,"Original Membership Branch Number"
,"Original Membership Branch Name"
,"Original Membership Region"
,"Original Membership Department Type"
,"Original Membership Department Group"
    from 
    temp."00a17358-0903-4f15-8267-fad2a453b5bd"
    );
    

    -- Tile Name: DNA|AcctAcctRole|History|OEMP --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/2c686d2a-d686-4245-b189-61472d16d516/details/overview 
    -- 
    create or replace temp table 
    temp."79b0a542-0316-4a61-811f-5aa5c17c0899"
    as(
    select * 
    -- from raw."DNA|AcctAcctRole|History|OEMP$2c686d2a-d686-4245-b189-61472d16d516"
    -- from webforms."DNA|AcctAcctRole|History|OEMP$2c686d2a-d686-4245-b189-61472d16d516"
    from transform."DNA|AcctAcctRole|History|OEMP$2c686d2a-d686-4245-b189-61472d16d516"
    );
    

    -- Tile Name: Filter OEMP --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."07c330ea-7e78-456c-b9b0-2aae74b6fce1"
    as(
    select 
    * 
    from 
    temp."79b0a542-0316-4a61-811f-5aa5c17c0899"
    where
    ("Action" <> ''Deleted'')
    );
    

    -- Tile Name: UP-EmployeeCommon_MaxEffDate --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/1613ecd4-9ec9-4e06-8cc6-73f67ed47c4a/details/overview 
    -- 
-- OMIT ROW SETTING -- 
-- "Pers #", Null Value
-- "Unique Pers/Org Number", Null Value
    create or replace temp table 
    temp."5ba9363d-ebe0-4b15-9f92-6040736a3ef3"
    as(
    select * 
replace (
cast("Pers #" as number) as "Pers #"
,"Unique Pers/Org Number" as "Unique Pers/Org Number"
)
    -- from raw."UP|EmployeeCommon|MaxEffDate$1613ecd4-9ec9-4e06-8cc6-73f67ed47c4a"
    -- from webforms."UP|EmployeeCommon|MaxEffDate$1613ecd4-9ec9-4e06-8cc6-73f67ed47c4a"
    from transform."UP|EmployeeCommon|MaxEffDate$1613ecd4-9ec9-4e06-8cc6-73f67ed47c4a"
    );
    

    -- Tile Name: UP Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."cf34aa37-f38d-46ae-9fb7-c2cbfdebbbab"
    as(
    select 
    "Unique Pers/Org Number"
,("Employee Full-Name") as "Original Membership Employee"
,("Emp #") as "Original Membership Employee Number"
,("Pers #") as "Original Membership Employee Person Number"
    from 
    temp."5ba9363d-ebe0-4b15-9f92-6040736a3ef3"
    );
    

    -- Tile Name: OEMP Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."7143c39b-0b00-4f7c-8c4f-5906297f5d57"
    as(
    select 
    "ACCTNBR"
,"Unique Pers/Org Number"
,"Effective Date"
,"Inactive Date"
    from 
    temp."07c330ea-7e78-456c-b9b0-2aae74b6fce1"
    );
    

    -- Tile Name: Inner Join OEMP --
    -- Tile Type: Inner
    create or replace temp table 
    temp."5449eab1-78f3-4f39-b9a4-0302bb42b52a"
    as(
    select t1.* 
    ,t2.* 
exclude (
"ACCTNBR"
)
rename (
"Unique Pers/Org Number" as "OEMP.Unique Pers/Org Number"
,"Effective Date" as "OEMP.Effective Date"
,"Inactive Date" as "OEMP.Inactive Date"
)
    from 
    temp."229db9c2-1d92-45f0-8233-93c774e85fe1" as t1
    inner join
    temp."7143c39b-0b00-4f7c-8c4f-5906297f5d57" as t2
    on 
    t1."Original Membership Account" = t2."ACCTNBR"
    );
    

    -- Tile Name: Filter to OEMP Dates --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."805fd0ee-02e7-4e5c-9c3f-20f3d31375ed"
    as(
    select 
    * 
    from 
    temp."5449eab1-78f3-4f39-b9a4-0302bb42b52a"
    where
    ("OEMP.Effective Date" <= TO_TIMESTAMP(CONCAT("Original Membership Date",''T23:59:59'')) AND 
("OEMP.Inactive Date" > TO_TIMESTAMP(CONCAT("Original Membership Date",''T23:59:59'')) OR "OEMP.Inactive Date" IS NULL))
    );
    

    -- Tile Name: OEMP Final Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."5b0d29ec-d0c7-4f3a-bdef-59913387cae7"
    as(
    select 
    "Unique Pers/Org Number"
,"OEMP.Unique Pers/Org Number"
    from 
    temp."805fd0ee-02e7-4e5c-9c3f-20f3d31375ed"
    );
    

    -- Tile Name: Join OEMP To Member --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."cb80bb24-43e2-4c94-90e4-c1cb542f7dd2"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
    from 
    temp."229db9c2-1d92-45f0-8233-93c774e85fe1" as t1
    left outer join
    temp."5b0d29ec-d0c7-4f3a-bdef-59913387cae7" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: AKC|New Memberships (Decoded) --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/4275d1c4-43de-42d8-9cbd-06f0eed7c3b8/details/overview 
    -- 
-- OMIT ROW SETTING -- 
-- "UserHostId2", Null Value
    create or replace temp table 
    temp."eee8828b-dd89-4571-9613-49e898938a4f"
    as(
    select * 
replace (
cast("ApplicationId" as number) as "ApplicationId"
,cast("UserHostId2" as number) as "UserHostId2"
)
    -- from raw."AKC|New Memberships (Decoded)$4275d1c4-43de-42d8-9cbd-06f0eed7c3b8"
    -- from webforms."AKC|New Memberships (Decoded)$4275d1c4-43de-42d8-9cbd-06f0eed7c3b8"
    from transform."AKC|New Memberships (Decoded)$4275d1c4-43de-42d8-9cbd-06f0eed7c3b8"
    );
    

    -- Tile Name: New Membership Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."596cbe9d-7b55-4a9e-8dbf-b3af0de2ff74"
    as(
    select 
    "Unique Pers/Org Number"
,"NewMembershipCreatedDate"
,"UserHostId2"
    from 
    temp."eee8828b-dd89-4571-9613-49e898938a4f"
    );
    

    -- Tile Name: New Memberships --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."fb49a0c6-1539-428a-a73f-467df3af2217"
    as(
    with cte0 as (
      select * exclude("NewMembershipCreatedDate")
      ,(DATE("NewMembershipCreatedDate")) as "NewMembershipCreatedDate" -- creates new column
      -- replace ((DATE("NewMembershipCreatedDate")) as "NewMembershipCreatedDate") -- overwrites existing column
      from 
      temp."596cbe9d-7b55-4a9e-8dbf-b3af0de2ff74"
      )
,cte1 as (
      select *
      ,(CONCAT(''P'',"UserHostId2")) as "OEMP.Unique Pers/Org Number" -- creates new column
      -- replace ((CONCAT(''P'',"UserHostId2")) as "OEMP.Unique Pers/Org Number") -- overwrites existing column
      from 
      cte0
      )
    select 
    * 
    from 
    cte1
    );
    

    -- Tile Name: No OEMP --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."ca2644da-7f12-4c30-a52f-c6259398b5fb"
    as(
    select 
    * 
    from 
    temp."cb80bb24-43e2-4c94-90e4-c1cb542f7dd2"
    where
    (NULLIF("OEMP.Unique Pers/Org Number",'''') IS NULL)
and ("Original Membership Type" = ''Primary'')
    );
    

    -- Tile Name: No OEMP Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."bbb2cfb9-2f2f-4e42-bdc2-545feb6c682c"
    as(
    select 
    "Unique Pers/Org Number"
    from 
    temp."ca2644da-7f12-4c30-a52f-c6259398b5fb"
    );
    

    -- Tile Name: Join New Primary Members --
    -- Tile Type: Inner
    create or replace temp table 
    temp."3ff64c95-4481-40e5-8276-5eb442ce9250"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
,"UserHostId2"
)
    from 
    temp."bbb2cfb9-2f2f-4e42-bdc2-545feb6c682c" as t1
    inner join
    temp."fb49a0c6-1539-428a-a73f-467df3af2217" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Join New Membership Data --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."435dcfb9-bbbf-4a73-9176-4a4e8e56a48e"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
rename (
"OEMP.Unique Pers/Org Number" as "AKC.OEMP.Unique Pers/Org Number"
)
    from 
    temp."cb80bb24-43e2-4c94-90e4-c1cb542f7dd2" as t1
    left outer join
    temp."3ff64c95-4481-40e5-8276-5eb442ce9250" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Coalesce --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."ea9054f7-2188-40d2-b9d4-3091538e77df"
    as(
    with cte0 as (
      select * exclude("OEMP.Unique Pers/Org Number")
      ,(IFNULL(NULLIF("OEMP.Unique Pers/Org Number",''''),"AKC.OEMP.Unique Pers/Org Number")) as "OEMP.Unique Pers/Org Number" -- creates new column
      -- replace ((IFNULL(NULLIF("OEMP.Unique Pers/Org Number",''''),"AKC.OEMP.Unique Pers/Org Number")) as "OEMP.Unique Pers/Org Number") -- overwrites existing column
      from 
      temp."435dcfb9-bbbf-4a73-9176-4a4e8e56a48e"
      )
    select 
    * 
    from 
    cte0
    );
    

    -- Tile Name: Join UP to OEMP --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."9cbd8860-00ba-4020-943f-f893d8b03e81"
    as(
    select t1.* 
exclude (
"OEMP.Unique Pers/Org Number"
,"AKC.OEMP.Unique Pers/Org Number"
)
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
    from 
    temp."ea9054f7-2188-40d2-b9d4-3091538e77df" as t1
    left outer join
    temp."cf34aa37-f38d-46ae-9fb7-c2cbfdebbbab" as t2
    on 
    t1."OEMP.Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: New Members --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."246d95ca-8fc9-4cfd-a4c0-986410e6bd0b"
    as(
    select 
    * 
    from 
    temp."9cbd8860-00ba-4020-943f-f893d8b03e81"
    where
    ("Original Membership Date" >= ''2018-02-01'')
and ("Original Membership Type" = ''Primary'')
and (STARTSWITH("Unique Pers/Org Number",''P''))
    );
    

    -- Tile Name: New members and Lost Members --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."a8b74614-4027-4c3b-a9fc-5198d15bed77"
    as(
    select 
    * 
    from 
    temp."59d57ff3-035e-4de6-bc38-ae011ed888dd"
    where
    ("Membership Tenure (Months)" = 0)
    );
    

    -- Tile Name: New --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."03011608-0ed2-45b1-ac54-d80d0271bd76"
    as(
    select 
    "EFFDATE"
,"Unique Pers/Org Number"
,"INCOMELEVCD"
,"INCOMELEVLOW"
,"INCOMELEVHIGH"
,"Generation (Detailed)"
,"Generation"
,"Age"
,"Membership Tenure"
,"Membership Tenure (Months)"
,"Membership Tenure (Days)"
,("Checking Member") as "New Checking Member"
,"Member Y/N Change"
,"ACH Deposit Amount"
,"Cash Deposit Amount"
,"Check Deposit Amount"
    from 
    temp."a8b74614-4027-4c3b-a9fc-5198d15bed77"
    );
    

    -- Tile Name: Join New --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."6d663d9c-c047-4f91-b1f8-3d68060038af"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
rename (
"ACH Deposit Amount" as "0 Month.ACH Deposit Amount"
,"Cash Deposit Amount" as "0 Month.Cash Deposit Amount"
,"Check Deposit Amount" as "0 Month.Check Deposit Amount"
)
    from 
    temp."246d95ca-8fc9-4cfd-a4c0-986410e6bd0b" as t1
    left outer join
    temp."03011608-0ed2-45b1-ac54-d80d0271bd76" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Last Month --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."f4298367-6380-4244-a905-8f2457709587"
    as(
    select 
    * 
    from 
    temp."59d57ff3-035e-4de6-bc38-ae011ed888dd"
    where
    (CASE 
WHEN "EFFDATE" = LAST_DAY(DATEADD(month,-1, CURRENT_DATE()))
THEN TRUE
ELSE FALSE
END)
    );
    

    -- Tile Name: Current --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."69a898b9-baa3-4829-a47e-8b238a248715"
    as(
    select 
    "Unique Pers/Org Number"
,"Direct Deposit Member"
,"Deposit Member"
,"Checking Member"
,"Debit Swipe Member"
,"Age"
,"Membership Tenure"
,"Membership Tenure (Months)"
,"Membership Tenure (Days)"
,"Member Y/N"
,"Joint Member Y/N"
,"EFFDATE"
    from 
    temp."f4298367-6380-4244-a905-8f2457709587"
    );
    

    -- Tile Name: 2 Month --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."0362169a-4e68-4e70-a46f-2eb1af52db29"
    as(
    select 
    "Unique Pers/Org Number"
,"Age"
,"Checking Member"
,"Debit Swipe Member"
,"Direct Deposit Member"
,"Deposit Member"
,"Member Y/N"
,"Joint Member Y/N"
,"EFFDATE"
,"ACH Deposit Amount"
,"Check Deposit Amount"
,"Cash Deposit Amount"
    from 
    temp."362151f1-e3d9-44cd-8552-d54709d70ccb"
    );
    

    -- Tile Name: 1 Month Members --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."0de30ff7-6132-4bc2-8d34-5e389cd12e73"
    as(
    select 
    * 
    from 
    temp."59d57ff3-035e-4de6-bc38-ae011ed888dd"
    where
    ("Membership Tenure (Months)" = 1)
    );
    

    -- Tile Name: 1 Month --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."8d2312a2-eeea-4932-97eb-b3013fe54781"
    as(
    select 
    "Unique Pers/Org Number"
,"Age"
,"Checking Member"
,"Debit Swipe Member"
,"Direct Deposit Member"
,"Deposit Member"
,"Member Y/N"
,"Joint Member Y/N"
,"EFFDATE"
,"ACH Deposit Amount"
,"Cash Deposit Amount"
,"Check Deposit Amount"
    from 
    temp."0de30ff7-6132-4bc2-8d34-5e389cd12e73"
    );
    

    -- Tile Name: Join 1 Month --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."253b6629-469a-4027-a652-48b130919f41"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
rename (
"EFFDATE" as "1 Month.EFFDATE"
,"Age" as "1 Month.Age"
,"Checking Member" as "1 Month.Checking Member"
,"Debit Swipe Member" as "1 Month.Debit Swipe Member"
,"Direct Deposit Member" as "1 Month.Direct Deposit Member"
,"Deposit Member" as "1 Month.Deposit Member"
,"Member Y/N" as "1 Month.Member Y/N"
,"Joint Member Y/N" as "1 Month.Joint Member Y/N"
,"ACH Deposit Amount" as "1 Month.ACH Deposit Amount"
,"Cash Deposit Amount" as "1 Month.Cash Deposit Amount"
,"Check Deposit Amount" as "1 Month.Check Deposit Amount"
)
    from 
    temp."6d663d9c-c047-4f91-b1f8-3d68060038af" as t1
    left outer join
    temp."8d2312a2-eeea-4932-97eb-b3013fe54781" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Join 2 Month --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."42a3a927-4d32-4e7e-84b4-0eb014b440c5"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
rename (
"Age" as "2 Month.Age"
,"Checking Member" as "2 Month.Checking Member"
,"Member Y/N" as "2 Month.Member Y/N"
,"Joint Member Y/N" as "2 Month.Joint Member Y/N"
,"Debit Swipe Member" as "2 Month.Debit Swipe Member"
,"Direct Deposit Member" as "2 Month.Direct Deposit Member"
,"Deposit Member" as "2 Month.Deposit Member"
,"EFFDATE" as "2 Month.EFFDATE"
,"ACH Deposit Amount" as "2 Month.ACH Deposit Amount"
,"Cash Deposit Amount" as "2 Month.Cash Deposit Amount"
,"Check Deposit Amount" as "2 Month.Check Deposit Amount"
)
    from 
    temp."253b6629-469a-4027-a652-48b130919f41" as t1
    left outer join
    temp."0362169a-4e68-4e70-a46f-2eb1af52db29" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Join Current --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."66f82625-276a-4cad-a203-ea6be02851c4"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
rename (
"Direct Deposit Member" as "Current.Direct Deposit Member"
,"Deposit Member" as "Current.Deposit Member"
,"Age" as "Current.Age"
,"Membership Tenure" as "Current.Membership Tenure"
,"Membership Tenure (Months)" as "Current.Membership Tenure (Months)"
,"Membership Tenure (Days)" as "Current.Membership Tenure (Days)"
,"Checking Member" as "Current.Checking Member"
,"Debit Swipe Member" as "Current.Debit Swipe Member"
,"Member Y/N" as "Current.Member Y/N"
,"Joint Member Y/N" as "Current.Joint Member Y/N"
,"EFFDATE" as "Current.EFFDATE"
)
    from 
    temp."42a3a927-4d32-4e7e-84b4-0eb014b440c5" as t1
    left outer join
    temp."69a898b9-baa3-4829-a47e-8b238a248715" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Coalesce 1 --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."17697059-dcbd-4e7c-81ae-a43c972c61c2"
    as(
    with cte0 as (
      select *
      /* ,(IFNULL("Current.Direct Deposit Member",0)) as "Current.Direct Deposit Member" */ -- creates new column
      replace ((IFNULL("Current.Direct Deposit Member",0)) as "Current.Direct Deposit Member") -- overwrites existing column
      from 
      temp."66f82625-276a-4cad-a203-ea6be02851c4"
      )
,cte1 as (
      select *
      /* ,(IFNULL("Current.Deposit Member",0)) as "Current.Deposit Member" */ -- creates new column
      replace ((IFNULL("Current.Deposit Member",0)) as "Current.Deposit Member") -- overwrites existing column
      from 
      cte0
      )
,cte2 as (
      select *
      /* ,(IFNULL("Current.Checking Member",0)) as "Current.Checking Member" */ -- creates new column
      replace ((IFNULL("Current.Checking Member",0)) as "Current.Checking Member") -- overwrites existing column
      from 
      cte1
      )
,cte3 as (
      select *
      /* ,(IFNULL("Current.Member Y/N",''N'')) as "Current.Member Y/N" */ -- creates new column
      replace ((IFNULL("Current.Member Y/N",''N'')) as "Current.Member Y/N") -- overwrites existing column
      from 
      cte2
      )
,cte4 as (
      select *
      /* ,(IFNULL("Current.Joint Member Y/N",''N'')) as "Current.Joint Member Y/N" */ -- creates new column
      replace ((IFNULL("Current.Joint Member Y/N",''N'')) as "Current.Joint Member Y/N") -- overwrites existing column
      from 
      cte3
      )
,cte5 as (
      select *
      ,("Current.Member Y/N" = ''Y'') as "Current Member" -- creates new column
      -- replace (("Current.Member Y/N" = ''Y'') as "Current Member") -- overwrites existing column
      from 
      cte4
      )
,cte6 as (
      select *
      ,("Current.Joint Member Y/N" = ''Y'') as "Current Joint Member" -- creates new column
      -- replace (("Current.Joint Member Y/N" = ''Y'') as "Current Joint Member") -- overwrites existing column
      from 
      cte5
      )
,cte7 as (
      select *
      ,(''Y'') as "New Member Y/N" -- creates new column
      -- replace ((''Y'') as "New Member Y/N") -- overwrites existing column
      from 
      cte6
      )
,cte8 as (
      select *
      ,(1) as "New Member" -- creates new column
      -- replace ((1) as "New Member") -- overwrites existing column
      from 
      cte7
      )
,cte9 as (
      select *
      ,("Original Membership Type" = ''Joint'') as "New Joint Member Y/N" -- creates new column
      -- replace (("Original Membership Type" = ''Joint'') as "New Joint Member Y/N") -- overwrites existing column
      from 
      cte8
      )
,cte10 as (
      select *
      ,(IFF("New Checking Member",''Y'',''N'')) as "New Checking Member Y/N"-- creates new column
      -- replace ((IF("New Checking Member",''Y'',''N'')) as "New Checking Member Y/N") -- overwrites existing column
      from 
      cte9
      )
,cte11 as (
      select *
      /* ,(IFNULL("2 Month.Checking Member",0)) as "2 Month.Checking Member" */ -- creates new column
      replace ((IFNULL("2 Month.Checking Member",0)) as "2 Month.Checking Member") -- overwrites existing column
      from 
      cte10
      )
,cte12 as (
      select *
      /* ,(IFNULL("2 Month.Member Y/N",''N'')) as "2 Month.Member Y/N" */ -- creates new column
      replace ((IFNULL("2 Month.Member Y/N",''N'')) as "2 Month.Member Y/N") -- overwrites existing column
      from 
      cte11
      )
,cte13 as (
      select *
      /* ,(IFNULL("2 Month.Joint Member Y/N",''N'')) as "2 Month.Joint Member Y/N" */ -- creates new column
      replace ((IFNULL("2 Month.Joint Member Y/N",''N'')) as "2 Month.Joint Member Y/N") -- overwrites existing column
      from 
      cte12
      )
,cte14 as (
      select *
      ,("2 Month.Member Y/N" = ''Y'') as "2 Month Member" -- creates new column
      -- replace (("2 Month.Member Y/N" = ''Y'') as "2 Month Member") -- overwrites existing column
      from 
      cte13
      )
,cte15 as (
      select *
      ,("2 Month.Joint Member Y/N" = ''Y'') as "2 Month Joint Member" -- creates new column
      -- replace (("2 Month.Joint Member Y/N" = ''Y'') as "2 Month Joint Member") -- overwrites existing column
      from 
      cte14
      )
,cte16 as (
      select *
      /* ,(IFNULL("1 Month.Checking Member",0)) as "1 Month.Checking Member" */ -- creates new column
      replace ((IFNULL("1 Month.Checking Member",0)) as "1 Month.Checking Member") -- overwrites existing column
      from 
      cte15
      )
,cte17 as (
      select *
      /* ,(IFNULL("1 Month.Member Y/N",''N'')) as "1 Month.Member Y/N" */ -- creates new column
      replace ((IFNULL("1 Month.Member Y/N",''N'')) as "1 Month.Member Y/N") -- overwrites existing column
      from 
      cte16
      )
,cte18 as (
      select *
      /* ,(IFNULL("1 Month.Joint Member Y/N",''N'')) as "1 Month.Joint Member Y/N" */ -- creates new column
      replace ((IFNULL("1 Month.Joint Member Y/N",''N'')) as "1 Month.Joint Member Y/N") -- overwrites existing column
      from 
      cte17
      )
,cte19 as (
      select *
      ,("1 Month.Member Y/N" = ''Y'') as "1 Month Member" -- creates new column
      -- replace (("1 Month.Member Y/N" = ''Y'') as "1 Month Member") -- overwrites existing column
      from 
      cte18
      )
,cte20 as (
      select *
      ,("1 Month.Joint Member Y/N" = ''Y'') as "1 Month Joint Member" -- creates new column
      -- replace (("1 Month.Joint Member Y/N" = ''Y'') as "1 Month Joint Member") -- overwrites existing column
      from 
      cte19
      )
,cte21 as (
      select *
      ,(IFNULL("0 Month.ACH Deposit Amount",0) + 
IFNULL("0 Month.Cash Deposit Amount",0) +
IFNULL("0 Month.Check Deposit Amount",0) >= 500) as "Leading Deposit Indicator" -- creates new column
      -- replace ((IFNULL("0 Month.ACH Deposit Amount",0) + IFNULL("0 Month.Cash Deposit Amount",0) + IFNULL("0 Month.Check Deposit Amount",0) >= 500) as "Leading Deposit Indicator") -- overwrites existing column
      from 
      cte20
      )
    select 
    * 
    from 
    cte21
    );
    

    -- Tile Name: Salesforce|Leads Common|MaxEffdate --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/c7284b85-ec61-4698-a5a9-4bda54eeffcf/details/overview 
    -- 
-- OMIT ROW SETTING -- 
-- "Unique Pers/Org Number", Null Value
-- "User.UltiPro Employee Number", Null Value
    create or replace temp table 
    temp."5e4ab5e9-0946-4bd4-a573-8034d31793d0"
    as(
    select * 
replace (
"Unique Pers/Org Number" as "Unique Pers/Org Number"
,cast("User.UltiPro Employee Number" as number) as "User.UltiPro Employee Number"
)
    -- from raw."Salesforce|Leads Common|MaxEffdate$c7284b85-ec61-4698-a5a9-4bda54eeffcf"
    -- from webforms."Salesforce|Leads Common|MaxEffdate$c7284b85-ec61-4698-a5a9-4bda54eeffcf"
    from transform."Salesforce|Leads Common|MaxEffdate$c7284b85-ec61-4698-a5a9-4bda54eeffcf"
    );
    

    -- Tile Name: Valid Leads --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."4fbab173-2cd2-4c7d-ba6d-f6d7f6def742"
    as(
    select 
    * 
    from 
    temp."5e4ab5e9-0946-4bd4-a573-8034d31793d0"
    where
    ("RecordType.Name" = ''Person Lead'')
and (IFNULL("IsDeleted",''false'') = ''false'')
and ("Status" = ''Closed - Won'')
    );
    

    -- Tile Name: Lead Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."9c6c6f2c-97be-4e3f-a4d7-ba079b4d9afd"
    as(
    select 
    "Id"
,"User.UltiPro Employee Number"
,"CreatedDate"
,"Unique Pers/Org Number"
,"ConvertedDate"
,"Product_SubType__c"
,"Status"
,"RecordType.Name"
    from 
    temp."4fbab173-2cd2-4c7d-ba6d-f6d7f6def742"
    );
    

    -- Tile Name: Key|Salesforce|Product Sub-Type --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/be48bd5f-8753-4f2f-9214-7e6d72e7e71d/details/overview 
    -- 
    create or replace temp table 
    temp."3965a041-c264-4932-9bd5-479385705844"
    as(
    select * 
    from raw."KEY|Salesforce|Product Sub-Type$be48bd5f-8753-4f2f-9214-7e6d72e7e71d"
    -- from webforms."KEY|Salesforce|Product Sub-Type$be48bd5f-8753-4f2f-9214-7e6d72e7e71d"
    -- from transform."KEY|Salesforce|Product Sub-Type$be48bd5f-8753-4f2f-9214-7e6d72e7e71d"
    );
    

    -- Tile Name: Salesforce Ck Leads --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."9212352a-69d6-4059-80e3-a70d65eb0650"
    as(
    select 
    * 
    from 
    temp."3965a041-c264-4932-9bd5-479385705844"
    where
    ("CHECKING" = ''Y'')
    );
    

    -- Tile Name: Salesforce Checking Sub Types --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."0b7a5146-44ae-43ea-91cd-5480bf5f75ff"
    as(
    select 
    "Product Sub Type"
    from 
    temp."9212352a-69d6-4059-80e3-a70d65eb0650"
    );
    

    -- Tile Name: Checking Leads --
    -- Tile Type: Inner
    create or replace temp table 
    temp."f188b1ce-9b8f-4edf-9ccb-ac19c1047884"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Product Sub Type"
)
    from 
    temp."9c6c6f2c-97be-4e3f-a4d7-ba079b4d9afd" as t1
    inner join
    temp."0b7a5146-44ae-43ea-91cd-5480bf5f75ff" as t2
    on 
    t1."Product_SubType__c" = t2."Product Sub Type"
    );
    

    -- Tile Name: Add Formula --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."97adba77-6fa8-4786-8a89-2e9f2d517bd9"
    as(
    with cte0 as (
      select *
      ,(DATE("CreatedDate")) as "Lead Created Date" -- creates new column
      -- replace ((DATE("CreatedDate")) as "Lead Created Date") -- overwrites existing column
      from 
      temp."f188b1ce-9b8f-4edf-9ccb-ac19c1047884"
      )
    select 
    * 
    from 
    cte0
    );
    

    -- Tile Name: Rank & Window --
    -- Tile Type: Rank & Window
    create or replace temp table 
    temp."6930cbc5-3b13-479c-9dff-15fe43455680"
    as(
    select 
    * 
    ,row_number() over (partition by "Unique Pers/Org Number", "User.UltiPro Employee Number", "Lead Created Date"  order by "CreatedDate" asc, "Id" asc) as "Order"
    from 
    temp."97adba77-6fa8-4786-8a89-2e9f2d517bd9"
    );
    

    -- Tile Name: First Lead Per Employee Per Day --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."acebf778-d5b1-49a0-94b3-e03056146869"
    as(
    select 
    * 
    from 
    temp."6930cbc5-3b13-479c-9dff-15fe43455680"
    where
    ("Order" = 1)
    );
    

    -- Tile Name: Salesforce Checking Leads --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."896b8abd-d933-4a67-a0f8-bab7dc9f98f9"
    as(
    select 
    "Unique Pers/Org Number"
,"Lead Created Date"
,("User.UltiPro Employee Number") as "Employee Number"
,("Id") as "Salesforce LeadId"
    from 
    temp."acebf778-d5b1-49a0-94b3-e03056146869"
    );
    

    -- Tile Name: Join Salesforce Checking Leads --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."dc663d91-706e-4187-86e9-8381562368ff"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
,"Lead Created Date"
,"Employee Number"
)
    from 
    temp."17697059-dcbd-4e7c-81ae-a43c972c61c2" as t1
    left outer join
    temp."896b8abd-d933-4a67-a0f8-bab7dc9f98f9" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
and t1."Original Membership Date" = t2."Lead Created Date"
and t1."Original Membership Employee Number" = t2."Employee Number"
    );
    

    -- Tile Name: DNA|Account|History --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/81ebfca0-2d87-4755-a704-80f2fecaf343/details/overview 
    -- 
-- OMIT ROW SETTING -- 
-- 
    create or replace temp table 
    temp."98aae52e-dd5c-443f-8e83-c1cc60b4980b"
    as(
    select * 
replace (
cast("ACCTNBR" as number) as "ACCTNBR"
,"Unique Pers/Org Number" as "Unique Pers/Org Number"
)
    -- from raw."DNA|Account|History$81ebfca0-2d87-4755-a704-80f2fecaf343"
    -- from webforms."DNA|Account|History$81ebfca0-2d87-4755-a704-80f2fecaf343"
    from transform."DNA|Account|History$81ebfca0-2d87-4755-a704-80f2fecaf343"
    );
    

    -- Tile Name: Acct Hist Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."7cddcf19-f254-47c5-b47d-3b5c125e4d9b"
    as(
    select 
    "ACCTNBR"
,"Action"
,"Effective Date"
,"Unique Pers/Org Number"
,"MJACCTTYPCD"
,"CURRMIACCTTYPCD"
,"PRODUCT"
    from 
    temp."98aae52e-dd5c-443f-8e83-c1cc60b4980b"
    );
    

    -- Tile Name: KEY|DNA|Product Type Translator --
    -- Tile Type: Input Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/57acec25-81ec-4ae8-9a1f-3efc46805747/details/overview 
    -- 
    create or replace temp table 
    temp."66060c8d-c34b-489d-af88-cd571c0c68c9"
    as(
    select * 
    from raw."Key|DNA|Product Type Translator$57acec25-81ec-4ae8-9a1f-3efc46805747"
    -- from webforms."Key|DNA|Product Type Translator$57acec25-81ec-4ae8-9a1f-3efc46805747"
    -- from transform."Key|DNA|Product Type Translator$57acec25-81ec-4ae8-9a1f-3efc46805747"
    );
    

    -- Tile Name: Consumer Checking Types --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."99dc4f12-75e5-4ebd-8ef3-8d564f958b1b"
    as(
    select 
    * 
    from 
    temp."66060c8d-c34b-489d-af88-cd571c0c68c9"
    where
    ("Consumer Account" = ''Y'' AND 
"CHECKING" = ''Y'')
    );
    

    -- Tile Name: Consumer Checking Type Columns --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."56df31c5-3424-4a48-a3a2-f5465184456b"
    as(
    select 
    "MJACCTTYPCD"
,"MIACCTTYPCD"
    from 
    temp."99dc4f12-75e5-4ebd-8ef3-8d564f958b1b"
    );
    

    -- Tile Name: Inner Join Consumer Checking Type --
    -- Tile Type: Inner
    create or replace temp table 
    temp."8005481c-32bf-4da2-b888-6f7008404380"
    as(
    select t1.* 
    ,t2.* 
exclude (
"MJACCTTYPCD"
,"MIACCTTYPCD"
)
    from 
    temp."7cddcf19-f254-47c5-b47d-3b5c125e4d9b" as t1
    inner join
    temp."56df31c5-3424-4a48-a3a2-f5465184456b" as t2
    on 
    t1."MJACCTTYPCD" = t2."MJACCTTYPCD"
and t1."CURRMIACCTTYPCD" = t2."MIACCTTYPCD"
    );
    

    -- Tile Name: Added Date (Date) --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."7ead9af0-441b-4bf3-a161-a86cec1890ef"
    as(
    with cte0 as (
      select *
      ,(DATE("Effective Date")) as "Account Added Date"-- creates new column
      -- replace ((DATE("Effective Date")) as "Account Added Date") -- overwrites existing column
      from 
      temp."8005481c-32bf-4da2-b888-6f7008404380"
      )
    select 
    * 
    from 
    cte0
    );
    

    -- Tile Name: Filter to Action Added --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."ee376474-d10f-47e2-8b43-ae29224e5fc2"
    as(
    select 
    * 
    from 
    temp."7ead9af0-441b-4bf3-a161-a86cec1890ef"
    where
    ("Action" = ''Added'')
    );
    

    -- Tile Name: New Member Columns for New Accounts --
    -- Tile Type: Select Columns
    create or replace temp table 
    temp."45619931-c6fc-4d20-b979-38870b524dd2"
    as(
    select 
    "Unique Pers/Org Number"
,"Original Membership Date"
,"Original Membership Type"
,"Salesforce LeadId"
    from 
    temp."dc663d91-706e-4187-86e9-8381562368ff"
    );
    

    -- Tile Name: Join Added Accounts --
    -- Tile Type: Inner
    create or replace temp table 
    temp."05d07aa4-60ab-4f87-8e09-b7c4ea39babd"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
    from 
    temp."45619931-c6fc-4d20-b979-38870b524dd2" as t1
    inner join
    temp."ee376474-d10f-47e2-8b43-ae29224e5fc2" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Potential Valid Date Ranges --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."882f352d-dea8-42e8-8a7c-9438cab80bff"
    as(
    select 
    * 
    from 
    temp."05d07aa4-60ab-4f87-8e09-b7c4ea39babd"
    where
    ("Account Added Date" >= "Original Membership Date" AND 
"Account Added Date" <= DATEADD(day, 30, "Original Membership Date"))
and ("Original Membership Type" = ''Primary'')
    );
    

    -- Tile Name: Translate --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."a985fef7-041a-41b0-a122-6d4ea7511b04"
    as(
    with cte0 as (
      select *
      ,(CASE WHEN "Account Added Date" = "Original Membership Date" THEN ''Membership Desk''
        WHEN NULLIF("Salesforce LeadId",'''') IS NOT NULL THEN ''Salesforce Lead'' END) as "Checking Conversion Type" -- creates new column
      -- replace ((CASE WHEN "Account Added Date" = "Original Membership Date" THEN ''Membership Desk''
      -- WHEN NULLIF("Salesforce LeadId",'''') IS NOT NULL THEN ''Salesforce Lead'' END) as "Checking Conversion Type") -- overwrites existing column
      from 
      temp."882f352d-dea8-42e8-8a7c-9438cab80bff"
      )
,cte1 as (
      select *
      ,("Checking Conversion Type" IS NOT NULL) as "Valid Checking Conversion" -- creates new column
      -- replace (("Checking Conversion Type" IS NOT NULL) as "Valid Checking Conversion") -- overwrites existing column
      from 
      cte0
      )
    select 
    * 
    from 
    cte1
    );
    

    -- Tile Name: Order Checking Conversions --
    -- Tile Type: Rank & Window
    create or replace temp table 
    temp."3887873a-e5d8-4c21-bd81-f029f1863e73"
    as(
    select 
    * 
    ,row_number() over (partition by "Unique Pers/Org Number", "Valid Checking Conversion"  order by "Effective Date" asc) as "Order"
    from 
    temp."a985fef7-041a-41b0-a122-6d4ea7511b04"
    );
    

    -- Tile Name: Filter Checking Conversions --
    -- Tile Type: Filter Rows
    create or replace temp table 
    temp."367d874c-af94-435e-9af1-bf69127cf110"
    as(
    select 
    * 
    from 
    temp."3887873a-e5d8-4c21-bd81-f029f1863e73"
    where
    ("Order" = 1 AND "Valid Checking Conversion")
    );
    

    -- Tile Name: Drop Columns Checking Conversions --
    -- Tile Type: Alter Columns
    -- 
    create or replace temp table 
    temp."161182a7-ffda-447c-8a2e-0bb2202f0e48"
    as(
    select 
    * 
exclude (
"Action"
,"Effective Date"
,"Order"
,"Salesforce LeadId"
,"Original Membership Type"
,"Original Membership Date"
,"MJACCTTYPCD"
,"CURRMIACCTTYPCD"
)  
rename (
"Account Added Date" as "Checking Account Added Date"
,"Valid Checking Conversion" as "Checking Conversion"
,"PRODUCT" as "Checking Product"
,"ACCTNBR" as "Checking Account Number"
)
    from 
    temp."367d874c-af94-435e-9af1-bf69127cf110"
    );
    

    -- Tile Name: Join Checking Conversions --
    -- Tile Type: Left Outer
    create or replace temp table 
    temp."6691b615-ab5e-4396-9a4f-4bf42200519f"
    as(
    select t1.* 
    ,t2.* 
exclude (
"Unique Pers/Org Number"
)
    from 
    temp."dc663d91-706e-4187-86e9-8381562368ff" as t1
    left outer join
    temp."161182a7-ffda-447c-8a2e-0bb2202f0e48" as t2
    on 
    t1."Unique Pers/Org Number" = t2."Unique Pers/Org Number"
    );
    

    -- Tile Name: Coalesce Checking Conversions --
    -- Tile Type: Add Formula
    create or replace temp table 
    temp."a6ddcf13-1f5f-4dc2-9867-37b42190ec47"
    as(
    with cte0 as (
      select *
      /* ,(IFNULL("Checking Conversion Type",''None'')) as "Checking Conversion Type" */ -- creates new column
      replace ((IFNULL("Checking Conversion Type",''None'')) as "Checking Conversion Type") -- overwrites existing column
      from 
      temp."6691b615-ab5e-4396-9a4f-4bf42200519f"
      )
,cte1 as (
      select *
      /* ,(IFNULL("Checking Conversion",FALSE)) as "Checking Conversion" */ -- creates new column
      replace ((IFNULL("Checking Conversion",FALSE)) as "Checking Conversion") -- overwrites existing column
      from 
      cte0
      )
,cte2 as (
      select *
      ,(IFF("Checking Conversion",''Y'',''N'')) as "Checking Conversion Y/N" -- creates new column
      -- replace ((IF("Checking Conversion",''Y'',''N'')) as "Checking Conversion Y/N") -- overwrites existing column
      from 
      cte1
      )
,cte3 as (
      select *
      ,(CONCAT(''*********'',LPAD(RIGHT("Original Membership Account",5),5,''*''))) as "Account Number (Last 5)" -- creates new column
      -- replace ((CONCAT(''*********'',LPAD(RIGHT("Original Membership Account",5),5,''*''))) as "Account Number (Last 5)") -- overwrites existing column
      from 
      cte2
      )
,cte4 as (
      select *
      ,(IFF("2 Month.Deposit Member",''Y'',''N'')) as "2 Month.Deposit Member Y/N" -- creates new column
      -- replace ((IF("2 Month.Deposit Member",''Y'',''N'')) as "2 Month.Deposit Member Y/N") -- overwrites existing column
      from 
      cte3
      )
,cte5 as (
      select *
      ,(IFF(LAST_DAY("Original Membership Date") = LAST_DAY(CURRENT_TIMESTAMP()),''Y'',''N'')) as "Membership Opened This Month" -- creates new column
      -- replace ((IF(LAST_DAY("Original Membership Date") = LAST_DAY(CURRENT_TIMESTAMP()),''Y'',''N'')) as "Membership Opened This Month") -- overwrites existing column
      from 
      cte4
      )
,cte6 as (
      select *
      ,(IFF("2 Month.EFFDATE" IS NOT NULL AND 
   "2 Month.EFFDATE" <> LAST_DAY(CURRENT_TIMESTAMP()),''Y'',''N'')) as "2 Full Months as Member" -- creates new column
      -- replace ((IF("2 Month.EFFDATE" IS NOT NULL AND "2 Month.EFFDATE" <> LAST_DAY(CURRENT_TIMESTAMP()),''Y'',''N'')) as "2 Full Months as Member") -- overwrites existing column
      from 
      cte5
      )
,cte7 as (
      select *
      ,(IFF("Leading Deposit Indicator",''Y'',''N'')) as "Leading Deposit Indicator Y/N" -- creates new column
      -- replace ((IF("Leading Deposit Indicator",''Y'',''N'')) as "Leading Deposit Indicator Y/N") -- overwrites existing column
      from 
      cte6
      )
,cte8 as (
      select *
      ,(CONCAT(''*********'',LPAD(RIGHT("Checking Account Number",5),5,''*''))) as "Checking Account Number (Last 5)" -- creates new column
      -- replace ((CONCAT(''*********'',LPAD(RIGHT("Checking Account Number",5),5,''*''))) as "Checking Account Number (Last 5)") -- overwrites existing column
      from 
      cte7
      )
,cte9 as (
      select *
      ,(IFF("Original Membership Branch Name" = ''UVU'',''Y'',''N'')) as "UVU Y/N" -- creates new column
      -- replace ((IF("Original Membership Branch Name" = ''UVU'',''Y'',''N'')) as "UVU Y/N") -- overwrites existing column
      from 
      cte8
      )
,cte10 as (
      select *
      ,(DATE("Original Membership Date")) as "Date" -- creates new column
      -- replace ((DATE("Original Membership Date")) as "Date") -- overwrites existing column
      from 
      cte9
      )
    select 
    * 
    from 
    cte10
    );
    

    -- Tile Name: KPIs|2022|New Members Direct Deposit Penetration --
    -- Tile Type: Output Dataset
    -- Dataset Link: https://uccu.domo.com/datasources/c885a65e-f08e-41f9-ba45-dc0af86e20d3/details/overview --
    create table if not exists DEPRECATED.TRANSFORM."KPIs|2022|New Members Direct Deposit Penetration$c885a65e-f08e-41f9-ba45-dc0af86e20d3" (
"Unique Pers/Org Number" varchar
,"Original Membership Account" number
,"Original Membership Date" date
,"Original Membership Type" varchar
,"UVU Membership Type" varchar
,"Original Membership Branch Number" number
,"Original Membership Branch Name" varchar
,"Original Membership Region" varchar
,"Original Membership Department Type" varchar
,"Original Membership Department Group" varchar
,"NewMembershipCreatedDate" date
,"Original Membership Employee" varchar
,"Original Membership Employee Number" number
,"Original Membership Employee Person Number" number
,"EFFDATE" date
,"INCOMELEVCD" number
,"INCOMELEVLOW" number
,"INCOMELEVHIGH" float
,"Generation (Detailed)" varchar
,"Generation" varchar
,"Age" number
,"Membership Tenure" number
,"Membership Tenure (Months)" number
,"Membership Tenure (Days)" number
,"New Checking Member" number
,"Member Y/N Change" number
,"0 Month.ACH Deposit Amount" float
,"0 Month.Cash Deposit Amount" float
,"0 Month.Check Deposit Amount" float
,"1 Month.Age" number
,"1 Month.Checking Member" number
,"1 Month.Debit Swipe Member" number
,"1 Month.Direct Deposit Member" number
,"1 Month.Deposit Member" number
,"1 Month.Member Y/N" varchar
,"1 Month.Joint Member Y/N" varchar
,"1 Month.EFFDATE" date
,"1 Month.ACH Deposit Amount" float
,"1 Month.Cash Deposit Amount" float
,"1 Month.Check Deposit Amount" float
,"2 Month.Age" number
,"2 Month.Checking Member" number
,"2 Month.Debit Swipe Member" number
,"2 Month.Direct Deposit Member" number
,"2 Month.Deposit Member" number
,"2 Month.Member Y/N" varchar
,"2 Month.Joint Member Y/N" varchar
,"2 Month.EFFDATE" date
,"2 Month.ACH Deposit Amount" float
,"2 Month.Check Deposit Amount" float
,"2 Month.Cash Deposit Amount" float
,"Current.Direct Deposit Member" number
,"Current.Deposit Member" number
,"Current.Checking Member" number
,"Current.Debit Swipe Member" number
,"Current.Age" number
,"Current.Membership Tenure" number
,"Current.Membership Tenure (Months)" number
,"Current.Membership Tenure (Days)" number
,"Current.Member Y/N" varchar
,"Current.Joint Member Y/N" varchar
,"Current.EFFDATE" date
,"Current Member" number
,"Current Joint Member" number
,"New Member Y/N" varchar
,"New Member" number
,"New Joint Member Y/N" number
,"New Checking Member Y/N" varchar
,"2 Month Member" number
,"2 Month Joint Member" number
,"1 Month Member" number
,"1 Month Joint Member" number
,"Leading Deposit Indicator" number
,"Salesforce LeadId" varchar
,"Checking Account Number" number
,"Checking Product" varchar
,"Checking Account Added Date" date
,"Checking Conversion Type" varchar
,"Checking Conversion" number
,"Checking Conversion Y/N" varchar
,"Account Number (Last 5)" varchar
,"2 Month.Deposit Member Y/N" varchar
,"Membership Opened This Month" varchar
,"2 Full Months as Member" varchar
,"Leading Deposit Indicator Y/N" varchar
,"Checking Account Number (Last 5)" varchar
,"UVU Y/N" varchar
,"Date" date) with tag(governance.tag.dataset_type = ''live'') comment = ''https://uccu.domo.com/datasources/c885a65e-f08e-41f9-ba45-dc0af86e20d3/details/overview'';
    
insert overwrite into
transform."KPIs|2022|New Members Direct Deposit Penetration$c885a65e-f08e-41f9-ba45-dc0af86e20d3"
(
    select 
    "Unique Pers/Org Number","Original Membership Account","Original Membership Date","Original Membership Type","UVU Membership Type","Original Membership Branch Number"
    ,"Original Membership Branch Name","Original Membership Region","Original Membership Department Type","Original Membership Department Group","NewMembershipCreatedDate"
    ,"Original Membership Employee","Original Membership Employee Number","Original Membership Employee Person Number", "EFFDATE", "INCOMELEVCD", "INCOMELEVLOW",
    "INCOMELEVHIGH","Generation (Detailed)","Generation","Age","Membership Tenure","Membership Tenure (Months)","Membership Tenure (Days)",
    "New Checking Member"::NUMBER(38,0) as "New Checking Member","Member Y/N Change","0 Month.ACH Deposit Amount","0 Month.Cash Deposit Amount","0 Month.Check Deposit Amount",
    "1 Month.Age","1 Month.Checking Member"::NUMBER(38,0) as "1 Month.Checking Member",
    "1 Month.Debit Swipe Member"::NUMBER(38,0) as "1 Month.Debit Swipe Member",
    "1 Month.Direct Deposit Member"::NUMBER(38,0) as "1 Month.Direct Deposit Member",
    "1 Month.Deposit Member"::NUMBER(38,0) as "1 Month.Deposit Member",
    "1 Month.Member Y/N","1 Month.Joint Member Y/N","1 Month.EFFDATE","1 Month.ACH Deposit Amount"
    ,"1 Month.Cash Deposit Amount","1 Month.Check Deposit Amount","2 Month.Age",
    "2 Month.Checking Member"::NUMBER(38,0) as "2 Month.Checking Member",
    "2 Month.Debit Swipe Member"::NUMBER(38,0) as "2 Month.Debit Swipe Member",
    "2 Month.Direct Deposit Member"::NUMBER(38,0) as "2 Month.Direct Deposit Member",
    "2 Month.Deposit Member"::NUMBER(38,0) as "2 Month.Deposit Member",
    "2 Month.Member Y/N","2 Month.Joint Member Y/N","2 Month.EFFDATE","2 Month.ACH Deposit Amount","2 Month.Check Deposit Amount","2 Month.Cash Deposit Amount",
    "Current.Direct Deposit Member"::NUMBER(38,0) as "Current.Direct Deposit Member",
    "Current.Deposit Member"::NUMBER(38,0) as "Current.Deposit Member",
    "Current.Checking Member"::NUMBER(38,0) as "Current.Checking Member",
    "Current.Debit Swipe Member"::NUMBER(38,0) as "Current.Debit Swipe Member",
    "Current.Age","Current.Membership Tenure","Current.Membership Tenure (Months)","Current.Membership Tenure (Days)","Current.Member Y/N", 
    "Current.Joint Member Y/N","Current.EFFDATE",
    "Current Member"::NUMBER(38,0) as "Current Member",
    "Current Joint Member"::NUMBER(38,0) as "Current Joint Member",
    "New Member Y/N","New Member",
    "New Joint Member Y/N"::NUMBER(38,0) as "New Joint Member Y/N","New Checking Member Y/N",
    "2 Month Member"::NUMBER(38,0) as "2 Month Member",
    "2 Month Joint Member"::NUMBER(38,0) as "2 Month Joint Member",
    "1 Month Member"::NUMBER(38,0) as "1 Month Member",
    "1 Month Joint Member"::NUMBER(38,0) as "1 Month Joint Member",
    "Leading Deposit Indicator"::NUMBER(38,0) as "Leading Deposit Indicator",
    "Salesforce LeadId","Checking Account Number","Checking Product","Checking Account Added Date","Checking Conversion Type",
    "Checking Conversion"::NUMBER(38,0) as "Checking Conversion",
    "Checking Conversion Y/N","Account Number (Last 5)","2 Month.Deposit Member Y/N","Membership Opened This Month","2 Full Months as Member",
    "Leading Deposit Indicator Y/N","Checking Account Number (Last 5)","UVU Y/N","Date"
    from 
    temp."a6ddcf13-1f5f-4dc2-9867-37b42190ec47"
);
    
return ''Done.'';
end';