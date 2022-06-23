from flask import Flask, request, jsonify
import data_connection as dc
import os

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))

#@app.route('/sample')
def assess_percent(qtr,busunit,process,geo,market):

    sqlSOX = """
    WITH Q1 AS
    (
    SELECT DISTINCT
    RPTG_QTR, 
    A.COUNTRY, 
    B.MARKET, 
    BUSINESS_UNIT, 
    PROCESS, 
    B.GEO, 
    A.ASSESSED_ENTITY_TYPE,  
    A.ASSESSED_ENTITY_ID,  
    RATING,
    CASE WHEN RATING = 'Satisfactory' THEN 1 ELSE 0 END AS SATISFACTORY,
    CASE WHEN RATING = 'Unsatisfactory' THEN 1 ELSE 0 END AS UNSAT,
    CASE WHEN RATING = 'Marginal' THEN 1 ELSE 0 END AS MARGINAL,
    CASE WHEN RATING = 'Exempt' OR EXEMPT = 'E' THEN 1 ELSE 0 END AS EXEMPTS,
    CASE WHEN RATING = 'Pending' THEN 1 ELSE 0 END AS PENDING,
    CASE WHEN RATING = 'Pending' OR EXEMPT = 'E' THEN 0 ELSE 1 END AS TOTAL_RATING
            
    FROM SOX.CFOPORT_ASSESSMENT A
    LEFT JOIN SOX.MARKET B ON A.COUNTRY = B.COUNTRY
    
    )

    SELECT DISTINCT 
    A.ASSESSED_ENTITY_TYPE,
    SUM(SATISFACTORY) AS SUM_SATISFACTORY,
    SUM(UNSAT) AS SUM_UNSATISFACTORY,
    SUM(MARGINAL) AS SUM_MARGINAL,
    SUM(EXEMPTS) AS SUM_EXEMPT,
    SUM(PENDING) AS SUM_PENDING,
    SUM(TOTAL_RATING) AS TOTAL_RATING,
    CAST((CAST(SUM(SATISFACTORY) AS DECIMAL(31,2)) / CAST(SUM(TOTAL_RATING) AS DECIMAL(31,2)) * 100) AS DECIMAL(31,2)) AS SAT_RATE

            
    FROM Q1 A
        
    WHERE A.RPTG_QTR IS NOT NULL
    AND A.TOTAL_RATING NOT IN (0)
    """

    sqlgroup = """

    GROUP BY A.ASSESSED_ENTITY_TYPE"""

    if busunit == "" and process == "" and market == "" and geo == "" and qtr == "":
        sqlSOX = sqlSOX + sqlgroup

    else:
        sqlSOX = concatFilter(sqlSOX, qtr, splitParam(qtr), 0, 'A.')
        sqlSOX = concatFilter(sqlSOX, busunit, splitParam(busunit), 1, 'A.')
        sqlSOX = concatFilter(sqlSOX, process, splitParam(process), 2, 'A.')
        sqlSOX = concatFilter(sqlSOX, geo, splitParam(geo), 3, 'A.')
        sqlSOX = concatFilter(sqlSOX, market, splitParam(market), 4, 'A.')

    sqlSOX = sqlSOX + sqlgroup
    
    # Execute the query
    assess_percent = list(dc.select_query(sqlSOX))

    print(assess_percent)

    return jsonify(assess_percent)

def concatFilter(sqlSOX, param, splitParam, cnt, alias): # Concatenate filters
    listParams = ['RPTG_QTR','BUSINESS_UNIT','PROCESS','GEO','MARKET']

    if param:
            sqlSOX +=" AND " + alias + listParams[cnt] + " IN (" + splitParam + ")"
    return sqlSOX


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