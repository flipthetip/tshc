from flask import Flask, request, jsonify
import json
import os
import pandas as pd
#from flask_sqlSOXalchemy import sqlSOXAlchemy

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))

#@app.route('/sample')


def filters_col(num):
    file = open('meta/'+str(num)+'.json')
    
    json_object = json.load(file)
    
    return json_object

def filters_process(qtr,busunit):

    sqlProcess = """
    WITH Q1 AS
    (
    SELECT DISTINCT RPTG_BUSINESS_UNIT AS BUSINESS_UNIT,RPTG_QTR,RPTG_PROCESS AS PROCESS FROM SOX.CFOPORT_CONTROL
        
    UNION
    SELECT DISTINCT RPTG_BUSINESS_UNIT AS BUSINESS_UNIT,RPTG_QTR,RPTG_PROCESS AS PROCESS FROM SOX.CFOPORT_RISK1

    UNION
    SELECT DISTINCT BUSINESS_UNIT,RPTG_QTR,PROCESS FROM SOX.CFOPORT_ASSESSMENT
    )

    SELECT PROCESS FROM Q1 WHERE BUSINESS_UNIT IN (""" + splitParam(busunit) + ") AND RPTG_QTR IN(" + splitParam(qtr) + ")" 

    process_filters = pd.DataFrame(dc.select_query(sqlProcess))
    filters_dict = {"Process": process_filters.values.flatten().tolist()}

    return filters_dict

def filters_geo(qtr,busunit,process):

    sqlGeo = """
    WITH Q1 AS
    (
    SELECT DISTINCT 
    BUSINESS_UNIT, RPTG_QTR, PROCESS, B.GEO
    FROM SOX.CFOPORT_ASSESSMENT A
    LEFT JOIN SOX.MARKET B ON A.COUNTRY = B.COUNTRY

    UNION

    SELECT                     
    RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_QTR, RPTG_PROCESS AS PROCESS, B.GEO        
    FROM SOX.CFOPORT_CONTROL A
    LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY

    UNION

    SELECT                  
    RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_QTR, RPTG_PROCESS AS PROCESS, B.GEO        
    FROM SOX.CFOPORT_RISK1 A
    LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY

    )

    SELECT DISTINCT GEO FROM Q1
    WHERE BUSINESS_UNIT IN (""" + splitParam(busunit) + ") AND PROCESS IN (" + splitParam(process) + ") AND RPTG_QTR IN(" + splitParam(qtr) + ")"
    
    geo_filters = pd.DataFrame(dc.select_query(sqlGeo))
    filters_dict = {"Geo": geo_filters.values.flatten().tolist()}

    return filters_dict

def filters_mkt(qtr,busunit,process,geo):

    sqlMarket = """
    WITH Q1 AS
    (
    SELECT DISTINCT 
    BUSINESS_UNIT, RPTG_QTR, PROCESS, B.GEO, B.MARKET
    FROM SOX.CFOPORT_ASSESSMENT A
    LEFT JOIN SOX.MARKET B ON A.COUNTRY = B.COUNTRY

    UNION

    SELECT                     
    RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_QTR, RPTG_PROCESS AS PROCESS, B.GEO, B.MARKET        
    FROM SOX.CFOPORT_CONTROL A
    LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY

    UNION

    SELECT                  
    RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_QTR, RPTG_PROCESS AS PROCESS, B.GEO, B.MARKET        
    FROM SOX.CFOPORT_RISK1 A
    LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY

    )

    SELECT DISTINCT MARKET FROM Q1
    WHERE BUSINESS_UNIT IN (""" + splitParam(busunit) + ") AND PROCESS IN (" + splitParam(process) + ") AND GEO IN (" + splitParam(geo) +\
    ") AND RPTG_QTR IN(""" + splitParam(qtr) + ")" 

    market_filters = pd.DataFrame(dc.select_query(sqlMarket))
    filters_dict = {"Market": market_filters.values.flatten().tolist()}

    return filters_dict

# def filters_country(busunit,process,geo,mkt):

#     sqlCountry = """
#     WITH Q1 AS
#     (
#     SELECT DISTINCT 
#     BUSINESS_UNIT, PROCESS, C.GEO, B.MARKET, A.COUNTRY
#     FROM SOX.CFOPORT_ASSESSMENT A
#     LEFT JOIN SOX.MARKET B ON A.COUNTRY = B.COUNTRY
#     LEFT JOIN SOX.GEO C ON B.GEO = C.WWBCIT_GEO

#     UNION

#     SELECT                     
#     RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_PROCESS AS PROCESS, C.GEO, B.MARKET, A.RPTG_COUNTRY AS COUNTRY        
#     FROM SOX.CFOPORT_CONTROL A
#     LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY
#     LEFT JOIN SOX.GEO C ON B.GEO = C.WWBCIT_GEO

#     UNION

#     SELECT                  
#     RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_PROCESS AS PROCESS, C.GEO, B.MARKET, A.RPTG_COUNTRY AS COUNTRY        
#     FROM SOX.CFOPORT_RISK1 A
#     LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY
#     LEFT JOIN SOX.GEO C ON B.GEO = C.WWBCIT_GEO
#     )

#     SELECT DISTINCT COUNTRY FROM Q1
#     WHERE BUSINESS_UNIT IN (""" + splitParam(busunit) + ") AND PROCESS IN (" + splitParam(process) + """)
#     AND GEO IN (""" + splitParam(geo) + ") AND MARKET IN (" + splitParam(mkt) + ")"


#     country_filters = pd.DataFrame(dc.select_query(sqlCountry))
#     filters_dict = {"Country": country_filters.values.flatten().tolist()}

#     return filters_dict



def filters_func(qtr,busunit,process,geo,mkt):

    sqlFunc = """
    WITH Q1 AS
    (
    SELECT                     
    RPTG_BUSINESS_UNIT AS BUSINESS_UNIT, RPTG_PROCESS AS PROCESS, B.GEO, B.MARKET, A.RPTG_QTR, FUNCTION_EXCUTG_CTRL        
    FROM SOX.CFOPORT_CONTROL A
    LEFT JOIN SOX.MARKET B ON A.RPTG_COUNTRY = B.COUNTRY

    )

    SELECT DISTINCT FUNCTION_EXCUTG_CTRL FROM Q1
    WHERE BUSINESS_UNIT IN (""" + splitParam(busunit) + ") AND PROCESS IN (" + splitParam(process) + """)
    AND GEO IN (""" + splitParam(geo) + ") AND MARKET IN (" + splitParam(mkt) + \
    ") AND RPTG_QTR IN(""" + splitParam(qtr) + ")"

    func_filters = pd.DataFrame(dc.select_query(sqlFunc))
    filters_dict = {"Function Executing Ctrl": func_filters.values.flatten().tolist()}

    return filters_dict


    # Execute the query
    # bu_filters = pd.DataFrame(dc.select_query(sqlBU))
    # process_filters = pd.DataFrame(dc.select_query(sqlProcess))
    # country_filters = pd.DataFrame(dc.select_query(sqlCountry))
    # market_filters = pd.DataFrame(dc.select_query(sqlMarket))
    # geo_filters = pd.DataFrame(dc.select_query(sqlGeo))
    # func_filters = pd.DataFrame(dc.select_query(sqlFunc))
    # qtr_filters = pd.DataFrame(dc.select_query(sqlQtr))

    # filters_dict = {"Business Unit": bu_filters.values.flatten().tolist(),"Process": process_filters.values.flatten().tolist(),"Country": country_filters.values.flatten().tolist(),\
    #     "Market": market_filters.values.flatten().tolist(),"Geo": geo_filters.values.flatten().tolist(),"Function Executing Ctrl": func_filters.values.flatten().tolist(),\
    #         "Quarter": qtr_filters.values.flatten().tolist()}


    # print(filters_dict)

    # return filters_dict

def splitParam(param): # Splitting multiple values for each parameters
    param = str(param)
    filters =param.split(',')
    itemfilter = ""
    for data in filters:
        if itemfilter == "":
            itemfilter = " '" + data +"' "
        else:
            itemfilter = itemfilter + " ,'" + data +"' "
        
            
    return itemfilter
