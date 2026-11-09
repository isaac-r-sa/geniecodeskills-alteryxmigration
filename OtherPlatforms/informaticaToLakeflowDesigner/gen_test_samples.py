#!/usr/bin/env python3
"""
Generate 5 synthetic Informatica PowerCenter mapping exports (POWERMART XML) for testing the
informatica-to-vdp skill:

  01_simple        m_LOAD_CUSTOMER_CLEAN         SQ -> Expression -> Filter -> Target
  02_medium        m_SALES_BY_CUSTOMER_MONTH     2 SQs (source filter) -> Joiner -> Expression -> Aggregator
  03_medium        m_ORDER_LINE_ROUTER           connected Lookup -> Expression (IIF/DECODE) -> Router (3 targets)
  04_complex       m_TOP_PRODUCTS_PER_STORE      Joiner + Lookup + Aggregator + Rank + unconnected Lookup + Sorter + Sequence
  05_very_complex  m_DIM_CUSTOMER_SCD2           Mapplet + MD5 + Lookup SQL override + Router + 2 Update Strategies
                                                 + shared Sequence + mapping parameter + workflow/session settings

Source tables match daphnedemos_catalog.migration_skill_tests.src_* (synthetic retail data).
Usage: python3 gen_test_samples.py <out_dir>
"""
import os
import sys
from xml.sax.saxutils import quoteattr

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")

# --------------------------------------------------------------------------- source/target catalog
COLS = {
    "CUSTOMERS": [("CUSTOMER_ID", "number", 10, 0), ("FIRST_NAME", "varchar2", 50, 0), ("LAST_NAME", "varchar2", 50, 0),
                  ("EMAIL", "varchar2", 120, 0), ("COUNTRY", "varchar2", 10, 0), ("SEGMENT", "varchar2", 20, 0),
                  ("SIGNUP_DATE", "varchar2", 10, 0), ("STATUS", "char", 1, 0)],
    "ORDERS": [("ORDER_ID", "number", 10, 0), ("CUSTOMER_ID", "number", 10, 0), ("ORDER_DATE", "date", 19, 0),
               ("CHANNEL", "varchar2", 10, 0), ("STORE_ID", "number", 10, 0), ("STATUS", "varchar2", 20, 0)],
    "ORDER_LINES": [("ORDER_ID", "number", 10, 0), ("LINE_NO", "number", 5, 0), ("PRODUCT_ID", "number", 10, 0),
                    ("QUANTITY", "number", 10, 0), ("UNIT_PRICE", "number", 12, 2), ("DISCOUNT_PCT", "number", 5, 2)],
    "PRODUCTS": [("PRODUCT_ID", "number", 10, 0), ("PRODUCT_NAME", "varchar2", 80, 0), ("CATEGORY", "varchar2", 30, 0),
                 ("LIST_PRICE", "number", 12, 2), ("COST", "number", 12, 2), ("ACTIVE_FLAG", "char", 1, 0)],
    "STORES": [("STORE_ID", "number", 10, 0), ("STORE_NAME", "varchar2", 50, 0), ("REGION", "varchar2", 20, 0), ("OPENED_DATE", "date", 19, 0)],
    "DIM_CUSTOMER": [("CUSTOMER_SK", "number", 10, 0), ("CUSTOMER_ID", "number", 10, 0), ("EMAIL", "varchar2", 120, 0),
                     ("SEGMENT", "varchar2", 20, 0), ("COUNTRY", "varchar2", 10, 0), ("VALID_FROM", "date", 19, 0),
                     ("VALID_TO", "date", 19, 0), ("CURRENT_FLAG", "char", 1, 0)],
    "CUSTOMER_UPDATES": [("CUSTOMER_ID", "number", 10, 0), ("EMAIL", "varchar2", 120, 0), ("SEGMENT", "varchar2", 20, 0),
                         ("COUNTRY", "varchar2", 10, 0), ("EXTRACT_DATE", "date", 19, 0)],
}
PCTYPE = {"number": "decimal", "varchar2": "string", "char": "string", "date": "date/time"}


def a(**kw):
    return " ".join(f"{k}={quoteattr(str(v))}" for k, v in kw.items())


def source(name, db="ERP_PROD", owner="ERP"):
    f = "\n".join(f'            <SOURCEFIELD {a(BUSINESSNAME="", DATATYPE=t, DESCRIPTION="", FIELDNUMBER=i + 1, KEYTYPE="PRIMARY KEY" if i == 0 else "NOT A KEY", NAME=n, NULLABLE="NOTNULL" if i == 0 else "NULL", PRECISION=p, SCALE=s)}/>'
                  for i, (n, t, p, s) in enumerate(COLS[name]))
    return f'        <SOURCE {a(BUSINESSNAME="", DATABASETYPE="Oracle", DBDNAME=db, DESCRIPTION="", NAME=name, OBJECTVERSION="1", OWNERNAME=owner, VERSIONNUMBER="1")}>\n{f}\n        </SOURCE>'


def target(name, fields):
    f = "\n".join(f'            <TARGETFIELD {a(BUSINESSNAME="", DATATYPE=t, DESCRIPTION="", FIELDNUMBER=i + 1, KEYTYPE="NOT A KEY", NAME=n, NULLABLE="NULL", PRECISION=p, SCALE=s)}/>'
                  for i, (n, t, p, s) in enumerate(fields))
    return f'        <TARGET {a(BUSINESSNAME="", CONSTRAINT="", DATABASETYPE="Oracle", DESCRIPTION="", NAME=name, OBJECTVERSION="1", TABLEOPTIONS="", VERSIONNUMBER="1")}>\n{f}\n        </TARGET>'


def tfield(n, porttype, dt="string", p=50, s=0, expr=None, **extra):
    kw = dict(DATATYPE=dt, DEFAULTVALUE="", DESCRIPTION="", NAME=n, PICTURETEXT="", PORTTYPE=porttype, PRECISION=p, SCALE=s)
    if expr is not None:
        kw.update(EXPRESSION=expr, EXPRESSIONTYPE=extra.pop("EXPRESSIONTYPE", "GENERAL"))
    kw.update(extra)
    return f'            <TRANSFORMFIELD {a(**kw)}/>'


def trans(name, ttype, fields, attrs=(), extra_xml="", reusable="NO", desc=""):
    at = "\n".join(f'            <TABLEATTRIBUTE {a(NAME=k, VALUE=v)}/>' for k, v in attrs)
    body = "\n".join(x for x in [extra_xml, "\n".join(fields), at] if x)
    return f'        <TRANSFORMATION {a(DESCRIPTION=desc, NAME=name, OBJECTVERSION="1", REUSABLE=reusable, TYPE=ttype, VERSIONNUMBER="1")}>\n{body}\n        </TRANSFORMATION>'


def sq(name, src, source_filter="", sql_query=""):
    fields = [tfield(n, "INPUT/OUTPUT", PCTYPE[t], p, s) for n, t, p, s in COLS[src]]
    attrs = [("Sql Query", sql_query), ("User Defined Join", ""), ("Source Filter", source_filter), ("Number Of Sorted Ports", "0"),
             ("Tracing Level", "Normal"), ("Select Distinct", "NO"), ("Pre SQL", ""), ("Post SQL", "")]
    return trans(name, "Source Qualifier", fields, attrs)


def conn(fi, ff, ti, tf, fit="Expression", tit="Expression"):
    return f'            <CONNECTOR {a(FROMFIELD=ff, FROMINSTANCE=fi, FROMINSTANCETYPE=fit, TOFIELD=tf, TOINSTANCE=ti, TOINSTANCETYPE=tit)}/>'


def conns(fi, ti, pairs, fit, tit):
    return [conn(fi, f, ti, t, fit, tit) for f, t in pairs]


def inst(name, tname, ttype, kind="TRANSFORMATION", assoc=None):
    inner = f'\n                <ASSOCIATED_SOURCE_INSTANCE {a(NAME=assoc)}/>\n            ' if assoc else ""
    return f'            <INSTANCE {a(DESCRIPTION="", NAME=name, REUSABLE="NO", TRANSFORMATION_NAME=tname, TRANSFORMATION_TYPE=ttype, TYPE=kind)}>{inner}</INSTANCE>'


def doc(folder_items, mapping_name, mapping_desc, transformations, instances, connectors, variables="", workflow="", mapplets=""):
    m = (f'        <MAPPING {a(DESCRIPTION=mapping_desc, ISVALID="YES", NAME=mapping_name, OBJECTVERSION="1", VERSIONNUMBER="1")}>\n'
         + "\n".join(transformations) + "\n" + "\n".join(instances) + "\n" + "\n".join(connectors)
         + ("\n" + variables if variables else "")
         + '\n            <ERPINFO/>\n        </MAPPING>')
    return ('<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">\n'
            '<POWERMART CREATION_DATE="10/30/2026 14:12:05" REPOSITORY_VERSION="186.95">\n'
            '<REPOSITORY NAME="REP_RETAIL_DEV" VERSION="186" CODEPAGE="UTF-8" DATABASETYPE="Oracle">\n'
            '    <FOLDER NAME="RETAIL_DWH" GROUP="" OWNER="infa_admin" SHARED="NOTSHARED" DESCRIPTION="Retail data warehouse loads" PERMISSIONS="rwx---r--" UUID="7f1c2a52-6b0e-4c55-9a1f-3c0d2b9e8a11">\n'
            + "\n".join(folder_items) + "\n" + (mapplets + "\n" if mapplets else "") + m + ("\n" + workflow if workflow else "")
            + '\n    </FOLDER>\n</REPOSITORY>\n</POWERMART>\n')


def write(sub, name, xml):
    d = os.path.join(OUT, sub)
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name + ".xml")
    open(p, "w").write(xml)
    print("wrote", p)


def session(mapping, sess, extra_attrs=(), targets=()):
    at = "\n".join(f'                <ATTRIBUTE {a(NAME=k, VALUE=v)}/>' for k, v in extra_attrs)
    ti = "\n".join(f'                <SESSTRANSFORMATIONINST {a(ISREPARTITIONPOINT="NO", PIPELINE="1", SINSTANCENAME=t, STAGE="1", TRANSFORMATIONNAME=t, TRANSFORMATIONTYPE="Target Definition")}/>' for t in targets)
    return (f'        <WORKFLOW {a(DESCRIPTION="", ISENABLED="YES", ISRUNNABLESERVICE="NO", ISSERVICE="NO", ISVALID="YES", NAME="wf_" + mapping[2:], REUSABLE_SCHEDULER="NO", SCHEDULERNAME="Scheduler", SERVERNAME="IS_RETAIL", SUSPEND_ON_ERROR="NO", TASKS_MUST_RUN_ON_SERVER="NO", VERSIONNUMBER="1")}>\n'
            f'            <SESSION {a(DESCRIPTION="", ISVALID="YES", MAPPINGNAME=mapping, NAME=sess, REUSABLE="NO", SORTORDER="Binary", VERSIONNUMBER="1")}>\n{ti}\n'
            f'                <ATTRIBUTE NAME="Treat source rows as" VALUE="Insert"/>\n{at}\n            </SESSION>\n'
            f'            <TASKINSTANCE {a(DESCRIPTION="", ISENABLED="YES", NAME="Start", REUSABLE="NO", TASKNAME="Start", TASKTYPE="Start")}/>\n'
            f'            <TASKINSTANCE {a(DESCRIPTION="", ISENABLED="YES", NAME=sess, REUSABLE="NO", TASKNAME=sess, TASKTYPE="Session")}/>\n'
            f'            <WORKFLOWLINK {a(CONDITION="", FROMTASK="Start", TOTASK=sess)}/>\n        </WORKFLOW>')


# ============================================================================ 01 simple
def m01():
    exp_fields = [tfield(n, "INPUT", PCTYPE[t], p, s) for n, t, p, s in COLS["CUSTOMERS"]] + [
        tfield("o_FIRST_NAME", "OUTPUT", expr="INITCAP(LTRIM(RTRIM(FIRST_NAME)))"),
        tfield("o_LAST_NAME", "OUTPUT", expr="INITCAP(LTRIM(RTRIM(LAST_NAME)))"),
        tfield("o_FULL_NAME", "OUTPUT", p=101, expr="INITCAP(LTRIM(RTRIM(FIRST_NAME))) || ' ' || INITCAP(LTRIM(RTRIM(LAST_NAME)))"),
        tfield("o_EMAIL", "OUTPUT", p=120, expr="LOWER(LTRIM(RTRIM(EMAIL)))"),
        tfield("o_COUNTRY", "OUTPUT", p=10, expr="UPPER(LTRIM(RTRIM(COUNTRY)))"),
        tfield("o_SEGMENT", "OUTPUT", p=20, expr="IIF(ISNULL(SEGMENT) OR LTRIM(RTRIM(SEGMENT)) = '', 'UNKNOWN', UPPER(LTRIM(RTRIM(SEGMENT))))"),
        tfield("o_SIGNUP_DATE", "OUTPUT", "date/time", 29, 9, expr="IIF(IS_DATE(SIGNUP_DATE, 'DD.MM.YYYY'), TO_DATE(SIGNUP_DATE, 'DD.MM.YYYY'), TO_DATE(SIGNUP_DATE, 'YYYY-MM-DD'))"),
    ]
    out_cols = ["CUSTOMER_ID", "o_FULL_NAME", "o_EMAIL", "o_COUNTRY", "o_SEGMENT", "o_SIGNUP_DATE", "STATUS"]
    fil_fields = [tfield(c, "INPUT/OUTPUT") for c in out_cols]
    tgt = [("CUSTOMER_ID", "number", 10, 0), ("FULL_NAME", "varchar2", 101, 0), ("EMAIL", "varchar2", 120, 0), ("COUNTRY", "varchar2", 10, 0),
           ("SEGMENT", "varchar2", 20, 0), ("SIGNUP_DATE", "date", 19, 0)]
    T = [sq("SQ_CUSTOMERS", "CUSTOMERS"),
         trans("EXP_STANDARDIZE", "Expression", exp_fields, [("Tracing Level", "Normal")], desc="Trim, case and default the customer attributes; parse two signup date formats."),
         trans("FIL_ACTIVE", "Filter", fil_fields, [("Filter Condition", "STATUS = 'A'"), ("Tracing Level", "Normal")], desc="Only active customers.")]
    I = [inst("CUSTOMERS", "CUSTOMERS", "Source Definition", "SOURCE"), inst("SQ_CUSTOMERS", "SQ_CUSTOMERS", "Source Qualifier", assoc="CUSTOMERS"),
         inst("EXP_STANDARDIZE", "EXP_STANDARDIZE", "Expression"), inst("FIL_ACTIVE", "FIL_ACTIVE", "Filter"),
         inst("T_CUSTOMER_CLEAN", "T_CUSTOMER_CLEAN", "Target Definition", "TARGET")]
    cols = [c[0] for c in COLS["CUSTOMERS"]]
    C = (conns("CUSTOMERS", "SQ_CUSTOMERS", [(c, c) for c in cols], "Source Definition", "Source Qualifier")
         + conns("SQ_CUSTOMERS", "EXP_STANDARDIZE", [(c, c) for c in cols], "Source Qualifier", "Expression")
         + conns("EXP_STANDARDIZE", "FIL_ACTIVE", [(c, c) for c in out_cols], "Expression", "Filter")
         + conns("FIL_ACTIVE", "T_CUSTOMER_CLEAN", [("CUSTOMER_ID", "CUSTOMER_ID"), ("o_FULL_NAME", "FULL_NAME"), ("o_EMAIL", "EMAIL"),
                                                   ("o_COUNTRY", "COUNTRY"), ("o_SEGMENT", "SEGMENT"), ("o_SIGNUP_DATE", "SIGNUP_DATE")], "Filter", "Target Definition"))
    write("01_simple_customer_clean", "m_LOAD_CUSTOMER_CLEAN",
          doc([source("CUSTOMERS"), target("T_CUSTOMER_CLEAN", tgt)], "m_LOAD_CUSTOMER_CLEAN", "Standardize customers and load active ones.", T, I, C,
              workflow=session("m_LOAD_CUSTOMER_CLEAN", "s_m_LOAD_CUSTOMER_CLEAN", [("Truncate target table option", "YES")], ["T_CUSTOMER_CLEAN"])))


# ============================================================================ 02 medium
def m02():
    o, l = [c[0] for c in COLS["ORDERS"]], [c[0] for c in COLS["ORDER_LINES"]]
    jnr = ([tfield(c, "INPUT/OUTPUT/MASTER", PCTYPE[t], p, s) for c, t, p, s in COLS["ORDERS"]]
           + [tfield(c + ("1" if c == "ORDER_ID" else ""), "INPUT/OUTPUT", PCTYPE[t], p, s) for c, t, p, s in COLS["ORDER_LINES"]])
    exp = [tfield(c, "INPUT/OUTPUT") for c in ["CUSTOMER_ID", "ORDER_ID", "LINE_NO", "QUANTITY", "ORDER_DATE"]] + [
        tfield("UNIT_PRICE", "INPUT", "decimal", 12, 2), tfield("DISCOUNT_PCT", "INPUT", "decimal", 5, 2),
        tfield("v_PRICE", "LOCAL VARIABLE", "decimal", 12, 2, expr="IIF(ISNULL(UNIT_PRICE), 0, UNIT_PRICE)"),
        tfield("o_LINE_AMOUNT", "OUTPUT", "decimal", 15, 4, expr="QUANTITY * v_PRICE * (1 - DISCOUNT_PCT)"),
        tfield("o_ORDER_MONTH", "OUTPUT", "string", 7, expr="TO_CHAR(ORDER_DATE, 'YYYY-MM')")]
    agg = [tfield("CUSTOMER_ID", "INPUT/OUTPUT", "decimal", 10, 0, EXPRESSIONTYPE="GROUPBY", expr="CUSTOMER_ID"),
           tfield("o_ORDER_MONTH", "INPUT/OUTPUT", "string", 7, EXPRESSIONTYPE="GROUPBY", expr="o_ORDER_MONTH"),
           tfield("o_LINE_AMOUNT", "INPUT", "decimal", 15, 4), tfield("QUANTITY", "INPUT", "decimal", 10, 0),
           tfield("LINE_NO", "INPUT", "decimal", 5, 0), tfield("ORDER_DATE", "INPUT", "date/time", 29, 9),
           tfield("TOTAL_AMOUNT", "OUTPUT", "decimal", 15, 2, expr="ROUND(SUM(o_LINE_AMOUNT), 2)"),
           tfield("TOTAL_QTY", "OUTPUT", "decimal", 12, 0, expr="SUM(QUANTITY)"),
           tfield("LINE_COUNT", "OUTPUT", "decimal", 10, 0, expr="COUNT(LINE_NO)"),
           tfield("LAST_ORDER_DATE", "OUTPUT", "date/time", 29, 9, expr="MAX(ORDER_DATE)")]
    tgt = [("CUSTOMER_ID", "number", 10, 0), ("ORDER_MONTH", "varchar2", 7, 0), ("TOTAL_AMOUNT", "number", 15, 2), ("TOTAL_QTY", "number", 12, 0),
           ("LINE_COUNT", "number", 10, 0), ("LAST_ORDER_DATE", "date", 19, 0)]
    T = [sq("SQ_ORDERS", "ORDERS", source_filter="ORDERS.STATUS = 'COMPLETED'"), sq("SQ_ORDER_LINES", "ORDER_LINES"),
         trans("JNR_ORDER_LINES", "Joiner", jnr, [("Join Condition", "ORDER_ID = ORDER_ID1"), ("Join Type", "Normal Join"),
                                                    ("Sorted Input", "NO"), ("Master Sort Order", "Auto"), ("Cache Directory", "$PMCacheDir")]),
         trans("EXP_LINE_AMOUNT", "Expression", exp),
         trans("AGG_CUSTOMER_MONTH", "Aggregator", agg, [("Sorted Input", "NO"), ("Aggregator Data Cache Size", "Auto")])]
    I = [inst("ORDERS", "ORDERS", "Source Definition", "SOURCE"), inst("ORDER_LINES", "ORDER_LINES", "Source Definition", "SOURCE"),
         inst("SQ_ORDERS", "SQ_ORDERS", "Source Qualifier", assoc="ORDERS"), inst("SQ_ORDER_LINES", "SQ_ORDER_LINES", "Source Qualifier", assoc="ORDER_LINES"),
         inst("JNR_ORDER_LINES", "JNR_ORDER_LINES", "Joiner"), inst("EXP_LINE_AMOUNT", "EXP_LINE_AMOUNT", "Expression"),
         inst("AGG_CUSTOMER_MONTH", "AGG_CUSTOMER_MONTH", "Aggregator"), inst("T_SALES_CUSTOMER_MONTH", "T_SALES_CUSTOMER_MONTH", "Target Definition", "TARGET")]
    C = (conns("ORDERS", "SQ_ORDERS", [(c, c) for c in o], "Source Definition", "Source Qualifier")
         + conns("ORDER_LINES", "SQ_ORDER_LINES", [(c, c) for c in l], "Source Definition", "Source Qualifier")
         + conns("SQ_ORDERS", "JNR_ORDER_LINES", [(c, c) for c in o], "Source Qualifier", "Joiner")
         + conns("SQ_ORDER_LINES", "JNR_ORDER_LINES", [(c, c + ("1" if c == "ORDER_ID" else "")) for c in l], "Source Qualifier", "Joiner")
         + conns("JNR_ORDER_LINES", "EXP_LINE_AMOUNT", [(c, c) for c in ["CUSTOMER_ID", "ORDER_ID", "LINE_NO", "QUANTITY", "ORDER_DATE", "UNIT_PRICE", "DISCOUNT_PCT"]], "Joiner", "Expression")
         + conns("EXP_LINE_AMOUNT", "AGG_CUSTOMER_MONTH", [(c, c) for c in ["CUSTOMER_ID", "o_ORDER_MONTH", "o_LINE_AMOUNT", "QUANTITY", "LINE_NO", "ORDER_DATE"]], "Expression", "Aggregator")
         + conns("AGG_CUSTOMER_MONTH", "T_SALES_CUSTOMER_MONTH", [("CUSTOMER_ID", "CUSTOMER_ID"), ("o_ORDER_MONTH", "ORDER_MONTH"), ("TOTAL_AMOUNT", "TOTAL_AMOUNT"),
                                                                  ("TOTAL_QTY", "TOTAL_QTY"), ("LINE_COUNT", "LINE_COUNT"), ("LAST_ORDER_DATE", "LAST_ORDER_DATE")], "Aggregator", "Target Definition"))
    write("02_medium_sales_by_customer_month", "m_SALES_BY_CUSTOMER_MONTH",
          doc([source("ORDERS"), source("ORDER_LINES"), target("T_SALES_CUSTOMER_MONTH", tgt)], "m_SALES_BY_CUSTOMER_MONTH",
              "Monthly sales per customer from completed orders.", T, I, C,
              workflow=session("m_SALES_BY_CUSTOMER_MONTH", "s_m_SALES_BY_CUSTOMER_MONTH", [("Truncate target table option", "YES")], ["T_SALES_CUSTOMER_MONTH"])))


# ============================================================================ 03 medium
def m03():
    lkp = [tfield("IN_PRODUCT_ID", "INPUT", "decimal", 10, 0), tfield("PRODUCT_ID", "LOOKUP", "decimal", 10, 0),
           tfield("LIST_PRICE", "LOOKUP/OUTPUT", "decimal", 12, 2), tfield("CATEGORY", "LOOKUP/OUTPUT", "string", 30), tfield("COST", "LOOKUP/OUTPUT", "decimal", 12, 2)]
    lkp_attrs = [("Lookup Sql Override", ""), ("Lookup table name", "PRODUCTS"), ("Lookup Source Filter", ""), ("Lookup caching enabled", "YES"),
                 ("Lookup policy on multiple match", "Use First Value"), ("Lookup condition", "PRODUCT_ID = IN_PRODUCT_ID"), ("Connection Information", "$Source")]
    pass_cols = ["ORDER_ID", "LINE_NO", "PRODUCT_ID", "QUANTITY", "DISCOUNT_PCT"]
    exp = [tfield(c, "INPUT/OUTPUT", "decimal", 12, 2) for c in pass_cols] + [
        tfield("UNIT_PRICE", "INPUT", "decimal", 12, 2), tfield("LIST_PRICE", "INPUT", "decimal", 12, 2), tfield("COST", "INPUT", "decimal", 12, 2),
        tfield("CATEGORY", "INPUT/OUTPUT", "string", 30),
        tfield("o_PRICE", "OUTPUT", "decimal", 12, 2, expr="IIF(ISNULL(UNIT_PRICE), LIST_PRICE, UNIT_PRICE)"),
        tfield("o_NET_AMOUNT", "OUTPUT", "decimal", 15, 2, expr="ROUND(QUANTITY * IIF(ISNULL(UNIT_PRICE), LIST_PRICE, UNIT_PRICE) * (1 - DISCOUNT_PCT), 2)"),
        tfield("o_MARGIN", "OUTPUT", "decimal", 15, 2, expr="ROUND(QUANTITY * IIF(ISNULL(UNIT_PRICE), LIST_PRICE, UNIT_PRICE) * (1 - DISCOUNT_PCT), 2) - ROUND(QUANTITY * COST, 2)"),
        tfield("o_CATEGORY_GROUP", "OUTPUT", "string", 10, expr="DECODE(CATEGORY, 'Dairy', 'FRESH', 'Bakery', 'FRESH', 'Produce', 'FRESH', 'Electronics', 'NON_FOOD', 'Household', 'NON_FOOD', 'DRY')")]
    rcols = pass_cols + ["CATEGORY", "o_PRICE", "o_NET_AMOUNT", "o_MARGIN", "o_CATEGORY_GROUP"]
    groups = [("INPUT", "INPUT", ""), ("HIGH_VALUE", "OUTPUT", "o_NET_AMOUNT >= 300"), ("DISCOUNTED", "OUTPUT", "DISCOUNT_PCT >= 0.25"), ("DEFAULT1", "OUTPUT/DEFAULT", "")]
    gxml = "\n".join(f'            <GROUP {a(DESCRIPTION="", EXPRESSION=e, NAME=g, ORDER=i + 1, TYPE=t)}/>' for i, (g, t, e) in enumerate(groups))
    rfields = []
    for gi, (g, t, _) in enumerate(groups):
        for c in rcols:
            nm = c if gi == 0 else f"{c}{gi}"
            rfields.append(tfield(nm, "INPUT" if gi == 0 else "OUTPUT", GROUP=g, **({} if gi == 0 else {"REF_FIELD": c})))
    tgt = [("ORDER_ID", "number", 10, 0), ("LINE_NO", "number", 5, 0), ("PRODUCT_ID", "number", 10, 0), ("CATEGORY", "varchar2", 30, 0),
           ("CATEGORY_GROUP", "varchar2", 10, 0), ("QUANTITY", "number", 10, 0), ("PRICE", "number", 12, 2), ("DISCOUNT_PCT", "number", 5, 2),
           ("NET_AMOUNT", "number", 15, 2), ("MARGIN", "number", 15, 2)]
    T = [sq("SQ_ORDER_LINES", "ORDER_LINES"), trans("LKP_PRODUCTS", "Lookup Procedure", lkp, lkp_attrs),
         trans("EXP_PRICING", "Expression", exp), trans("RTR_LINE_TYPE", "Router", rfields, [("Tracing Level", "Normal")], extra_xml=gxml)]
    I = [inst("ORDER_LINES", "ORDER_LINES", "Source Definition", "SOURCE"), inst("SQ_ORDER_LINES", "SQ_ORDER_LINES", "Source Qualifier", assoc="ORDER_LINES"),
         inst("LKP_PRODUCTS", "LKP_PRODUCTS", "Lookup Procedure"), inst("EXP_PRICING", "EXP_PRICING", "Expression"), inst("RTR_LINE_TYPE", "RTR_LINE_TYPE", "Router")]
    tmap = lambda sfx: [(f"ORDER_ID{sfx}", "ORDER_ID"), (f"LINE_NO{sfx}", "LINE_NO"), (f"PRODUCT_ID{sfx}", "PRODUCT_ID"), (f"CATEGORY{sfx}", "CATEGORY"),
                        (f"o_CATEGORY_GROUP{sfx}", "CATEGORY_GROUP"), (f"QUANTITY{sfx}", "QUANTITY"), (f"o_PRICE{sfx}", "PRICE"),
                        (f"DISCOUNT_PCT{sfx}", "DISCOUNT_PCT"), (f"o_NET_AMOUNT{sfx}", "NET_AMOUNT"), (f"o_MARGIN{sfx}", "MARGIN")]
    l = [c[0] for c in COLS["ORDER_LINES"]]
    C = (conns("ORDER_LINES", "SQ_ORDER_LINES", [(c, c) for c in l], "Source Definition", "Source Qualifier")
         + [conn("SQ_ORDER_LINES", "PRODUCT_ID", "LKP_PRODUCTS", "IN_PRODUCT_ID", "Source Qualifier", "Lookup Procedure")]
         + conns("SQ_ORDER_LINES", "EXP_PRICING", [(c, c) for c in pass_cols + ["UNIT_PRICE"]], "Source Qualifier", "Expression")
         + conns("LKP_PRODUCTS", "EXP_PRICING", [("LIST_PRICE", "LIST_PRICE"), ("CATEGORY", "CATEGORY"), ("COST", "COST")], "Lookup Procedure", "Expression")
         + conns("EXP_PRICING", "RTR_LINE_TYPE", [(c, c) for c in rcols], "Expression", "Router"))
    tg = []
    for gi, tname in [(1, "T_LINES_HIGH_VALUE"), (2, "T_LINES_DISCOUNTED"), (3, "T_LINES_STANDARD")]:
        I.append(inst(tname, tname, "Target Definition", "TARGET"))
        C += conns("RTR_LINE_TYPE", tname, tmap(str(gi)), "Router", "Target Definition")
        tg.append(target(tname, tgt))
    write("03_medium_order_line_router", "m_ORDER_LINE_ROUTER",
          doc([source("ORDER_LINES"), source("PRODUCTS")] + tg, "m_ORDER_LINE_ROUTER",
              "Price order lines via product lookup and route them to high-value / discounted / standard targets.", T, I, C,
              workflow=session("m_ORDER_LINE_ROUTER", "s_m_ORDER_LINE_ROUTER", [("Truncate target table option", "YES")], ["T_LINES_HIGH_VALUE", "T_LINES_DISCOUNTED", "T_LINES_STANDARD"])))


# ============================================================================ 04 complex
def m04():
    o, l = [c[0] for c in COLS["ORDERS"]], [c[0] for c in COLS["ORDER_LINES"]]
    jnr = ([tfield(c, "INPUT/OUTPUT/MASTER", PCTYPE[t], p, s) for c, t, p, s in COLS["ORDERS"]]
           + [tfield(c + ("1" if c == "ORDER_ID" else ""), "INPUT/OUTPUT", PCTYPE[t], p, s) for c, t, p, s in COLS["ORDER_LINES"]])
    lkp = [tfield("IN_PRODUCT_ID", "INPUT", "decimal", 10, 0), tfield("PRODUCT_ID", "LOOKUP", "decimal", 10, 0),
           tfield("PRODUCT_NAME", "LOOKUP/OUTPUT", "string", 80), tfield("CATEGORY", "LOOKUP/OUTPUT", "string", 30), tfield("LIST_PRICE", "LOOKUP/OUTPUT", "decimal", 12, 2)]
    lkp_attrs = [("Lookup table name", "PRODUCTS"), ("Lookup condition", "PRODUCT_ID = IN_PRODUCT_ID"), ("Lookup policy on multiple match", "Use First Value"), ("Lookup caching enabled", "YES")]
    ulkp = [tfield("IN_STORE_ID", "INPUT", "decimal", 10, 0), tfield("STORE_ID", "LOOKUP", "decimal", 10, 0),
            tfield("STORE_NAME", "LOOKUP/OUTPUT", "string", 50), tfield("REGION", "LOOKUP/OUTPUT/RETURN", "string", 20)]
    ulkp_attrs = [("Lookup table name", "STORES"), ("Lookup condition", "STORE_ID = IN_STORE_ID"), ("Lookup policy on multiple match", "Use First Value")]
    exp1 = [tfield(c, "INPUT/OUTPUT", "decimal", 10, 0) for c in ["STORE_ID", "PRODUCT_ID", "QUANTITY"]] + [
        tfield("UNIT_PRICE", "INPUT", "decimal", 12, 2), tfield("DISCOUNT_PCT", "INPUT", "decimal", 5, 2), tfield("LIST_PRICE", "INPUT", "decimal", 12, 2),
        tfield("PRODUCT_NAME", "INPUT/OUTPUT", "string", 80), tfield("CATEGORY", "INPUT/OUTPUT", "string", 30),
        tfield("o_NET_AMOUNT", "OUTPUT", "decimal", 15, 2, expr="ROUND(QUANTITY * IIF(ISNULL(UNIT_PRICE), LIST_PRICE, UNIT_PRICE) * (1 - DISCOUNT_PCT), 2)")]
    agg = [tfield("STORE_ID", "INPUT/OUTPUT", "decimal", 10, 0, EXPRESSIONTYPE="GROUPBY", expr="STORE_ID"),
           tfield("PRODUCT_ID", "INPUT/OUTPUT", "decimal", 10, 0, EXPRESSIONTYPE="GROUPBY", expr="PRODUCT_ID"),
           tfield("PRODUCT_NAME", "INPUT/OUTPUT", "string", 80), tfield("CATEGORY", "INPUT/OUTPUT", "string", 30),
           tfield("o_NET_AMOUNT", "INPUT", "decimal", 15, 2), tfield("QUANTITY", "INPUT", "decimal", 10, 0),
           tfield("REVENUE", "OUTPUT", "decimal", 15, 2, expr="ROUND(SUM(o_NET_AMOUNT), 2)"), tfield("UNITS", "OUTPUT", "decimal", 12, 0, expr="SUM(QUANTITY)")]
    rnk = [tfield("RANKINDEX", "OUTPUT", "decimal", 10, 0),
           tfield("STORE_ID", "INPUT/OUTPUT", "decimal", 10, 0, EXPRESSIONTYPE="GROUPBY", expr="STORE_ID"),
           tfield("PRODUCT_ID", "INPUT/OUTPUT", "decimal", 10, 0), tfield("PRODUCT_NAME", "INPUT/OUTPUT", "string", 80),
           tfield("CATEGORY", "INPUT/OUTPUT", "string", 30), tfield("UNITS", "INPUT/OUTPUT", "decimal", 12, 0),
           tfield("REVENUE", "INPUT/OUTPUT", "decimal", 15, 2, ISRANKPORT="YES")]
    rnk_attrs = [("Top/Bottom", "Top"), ("Number Of Ranks", "3"), ("Case Sensitive String Comparison", "YES"), ("Rank Data Cache Size", "Auto")]
    exp2 = [tfield(c, "INPUT/OUTPUT", "string", 80) for c in ["RANKINDEX", "STORE_ID", "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "UNITS", "REVENUE"]] + [
        tfield("o_STORE_NAME", "OUTPUT", "string", 50, expr="IIF(ISNULL(:LKP.LKP_STORE(STORE_ID)), 'UNKNOWN', :LKP.LKP_STORE(STORE_ID))"),
        tfield("o_REGION", "OUTPUT", "string", 20, expr=":LKP.LKP_STORE_REGION(STORE_ID)")]
    srt_cols = ["STORE_ID", "RANKINDEX", "PRODUCT_ID", "o_STORE_NAME", "o_REGION", "PRODUCT_NAME", "CATEGORY", "UNITS", "REVENUE"]
    srt = [tfield(c, "INPUT/OUTPUT", ISSORTKEY="YES" if c in ("STORE_ID", "RANKINDEX", "PRODUCT_ID") else "NO",
                  SORTDIRECTION="ASCENDING") for c in srt_cols]
    seq = [tfield("NEXTVAL", "OUTPUT", "bigint", 19, 0), tfield("CURRVAL", "OUTPUT", "bigint", 19, 0)]
    seq_attrs = [("Start Value", "1000"), ("Increment By", "1"), ("End Value", "9223372036854775807"), ("Current Value", "1000"), ("Cycle", "NO"), ("Reset", "YES")]
    tgt = [("REPORT_ROW_ID", "number", 19, 0), ("STORE_ID", "number", 10, 0), ("STORE_NAME", "varchar2", 50, 0), ("REGION", "varchar2", 20, 0),
           ("RANK_IN_STORE", "number", 5, 0), ("PRODUCT_ID", "number", 10, 0), ("PRODUCT_NAME", "varchar2", 80, 0), ("CATEGORY", "varchar2", 30, 0),
           ("UNITS", "number", 12, 0), ("REVENUE", "number", 15, 2)]
    T = [sq("SQ_ORDERS", "ORDERS", source_filter="ORDERS.STATUS = 'COMPLETED' AND ORDERS.CHANNEL = 'STORE'"), sq("SQ_ORDER_LINES", "ORDER_LINES"),
         trans("JNR_ORDERS_LINES", "Joiner", jnr, [("Join Condition", "ORDER_ID = ORDER_ID1"), ("Join Type", "Normal Join")]),
         trans("LKP_PRODUCTS", "Lookup Procedure", lkp, lkp_attrs),
         trans("LKP_STORE", "Lookup Procedure", [tfield("IN_STORE_ID", "INPUT", "decimal", 10, 0), tfield("STORE_ID", "LOOKUP", "decimal", 10, 0), tfield("STORE_NAME", "LOOKUP/OUTPUT/RETURN", "string", 50)],
               [("Lookup table name", "STORES"), ("Lookup condition", "STORE_ID = IN_STORE_ID")], desc="Unconnected lookup: store name"),
         trans("LKP_STORE_REGION", "Lookup Procedure", ulkp, ulkp_attrs, desc="Unconnected lookup: store region"),
         trans("EXP_NET_AMOUNT", "Expression", exp1), trans("AGG_STORE_PRODUCT", "Aggregator", agg),
         trans("RNK_TOP3_PER_STORE", "Rank", rnk, rnk_attrs, desc="Top 3 products by revenue per store (ties share a rank)."),
         trans("EXP_STORE_ATTRS", "Expression", exp2, desc="Unconnected lookups for store name and region."),
         trans("SRT_REPORT", "Sorter", srt, [("Distinct", "NO"), ("Case Sensitive", "YES"), ("Null Treated Low", "NO")]),
         trans("SEQ_REPORT_ROW", "Sequence", seq, seq_attrs)]
    I = [inst("ORDERS", "ORDERS", "Source Definition", "SOURCE"), inst("ORDER_LINES", "ORDER_LINES", "Source Definition", "SOURCE"),
         inst("SQ_ORDERS", "SQ_ORDERS", "Source Qualifier", assoc="ORDERS"), inst("SQ_ORDER_LINES", "SQ_ORDER_LINES", "Source Qualifier", assoc="ORDER_LINES")]
    I += [inst(n, n, t) for n, t in [("JNR_ORDERS_LINES", "Joiner"), ("LKP_PRODUCTS", "Lookup Procedure"), ("LKP_STORE", "Lookup Procedure"),
                                      ("LKP_STORE_REGION", "Lookup Procedure"), ("EXP_NET_AMOUNT", "Expression"), ("AGG_STORE_PRODUCT", "Aggregator"),
                                      ("RNK_TOP3_PER_STORE", "Rank"), ("EXP_STORE_ATTRS", "Expression"), ("SRT_REPORT", "Sorter"), ("SEQ_REPORT_ROW", "Sequence")]]
    I.append(inst("T_TOP_PRODUCTS_STORE", "T_TOP_PRODUCTS_STORE", "Target Definition", "TARGET"))
    C = (conns("ORDERS", "SQ_ORDERS", [(c, c) for c in o], "Source Definition", "Source Qualifier")
         + conns("ORDER_LINES", "SQ_ORDER_LINES", [(c, c) for c in l], "Source Definition", "Source Qualifier")
         + conns("SQ_ORDERS", "JNR_ORDERS_LINES", [(c, c) for c in o], "Source Qualifier", "Joiner")
         + conns("SQ_ORDER_LINES", "JNR_ORDERS_LINES", [(c, c + ("1" if c == "ORDER_ID" else "")) for c in l], "Source Qualifier", "Joiner")
         + [conn("JNR_ORDERS_LINES", "PRODUCT_ID", "LKP_PRODUCTS", "IN_PRODUCT_ID", "Joiner", "Lookup Procedure")]
         + conns("JNR_ORDERS_LINES", "EXP_NET_AMOUNT", [(c, c) for c in ["STORE_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "DISCOUNT_PCT"]], "Joiner", "Expression")
         + conns("LKP_PRODUCTS", "EXP_NET_AMOUNT", [("PRODUCT_NAME", "PRODUCT_NAME"), ("CATEGORY", "CATEGORY"), ("LIST_PRICE", "LIST_PRICE")], "Lookup Procedure", "Expression")
         + conns("EXP_NET_AMOUNT", "AGG_STORE_PRODUCT", [(c, c) for c in ["STORE_ID", "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "o_NET_AMOUNT", "QUANTITY"]], "Expression", "Aggregator")
         + conns("AGG_STORE_PRODUCT", "RNK_TOP3_PER_STORE", [(c, c) for c in ["STORE_ID", "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "UNITS", "REVENUE"]], "Aggregator", "Rank")
         + conns("RNK_TOP3_PER_STORE", "EXP_STORE_ATTRS", [(c, c) for c in ["RANKINDEX", "STORE_ID", "PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "UNITS", "REVENUE"]], "Rank", "Expression")
         + conns("EXP_STORE_ATTRS", "SRT_REPORT", [(c, c) for c in srt_cols], "Expression", "Sorter")
         + conns("SRT_REPORT", "T_TOP_PRODUCTS_STORE", [("STORE_ID", "STORE_ID"), ("o_STORE_NAME", "STORE_NAME"), ("o_REGION", "REGION"), ("RANKINDEX", "RANK_IN_STORE"),
                                                        ("PRODUCT_ID", "PRODUCT_ID"), ("PRODUCT_NAME", "PRODUCT_NAME"), ("CATEGORY", "CATEGORY"), ("UNITS", "UNITS"), ("REVENUE", "REVENUE")], "Sorter", "Target Definition")
         + [conn("SEQ_REPORT_ROW", "NEXTVAL", "T_TOP_PRODUCTS_STORE", "REPORT_ROW_ID", "Sequence", "Target Definition")])
    write("04_complex_top_products_per_store", "m_TOP_PRODUCTS_PER_STORE",
          doc([source("ORDERS"), source("ORDER_LINES"), source("PRODUCTS"), source("STORES"), target("T_TOP_PRODUCTS_STORE", tgt)],
              "m_TOP_PRODUCTS_PER_STORE", "Top 3 products by revenue per store for completed in-store orders, numbered by a sequence.", T, I, C,
              workflow=session("m_TOP_PRODUCTS_PER_STORE", "s_m_TOP_PRODUCTS_PER_STORE", [("Truncate target table option", "YES")], ["T_TOP_PRODUCTS_STORE"])))


# ============================================================================ 05 very complex
def m05():
    # reusable mapplet: standardize customer attributes
    mplt = ('        <MAPPLET ' + a(DESCRIPTION="Standardize email / segment / country (shared across customer loads).", ISVALID="YES", NAME="mplt_STD_CUSTOMER", OBJECTVERSION="1", VERSIONNUMBER="1") + '>\n'
            + trans("IN_STD", "Input Transformation", [tfield(c, "OUTPUT") for c in ["CUSTOMER_ID", "EMAIL", "SEGMENT", "COUNTRY"]]) + "\n"
            + trans("EXP_STD", "Expression", [tfield(c, "INPUT") for c in ["CUSTOMER_ID", "EMAIL", "SEGMENT", "COUNTRY"]] + [
                tfield("o_CUSTOMER_ID", "OUTPUT", "decimal", 10, 0, expr="CUSTOMER_ID"),
                tfield("o_EMAIL", "OUTPUT", p=120, expr="LOWER(LTRIM(RTRIM(EMAIL)))"),
                tfield("o_SEGMENT", "OUTPUT", p=20, expr="IIF(ISNULL(SEGMENT) OR LTRIM(RTRIM(SEGMENT)) = '', 'UNKNOWN', UPPER(LTRIM(RTRIM(SEGMENT))))"),
                tfield("o_COUNTRY", "OUTPUT", p=10, expr="UPPER(LTRIM(RTRIM(COUNTRY)))")]) + "\n"
            + trans("OUT_STD", "Output Transformation", [tfield(c, "INPUT") for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY"]]) + "\n"
            + "\n".join([inst("IN_STD", "IN_STD", "Input Transformation"), inst("EXP_STD", "EXP_STD", "Expression"), inst("OUT_STD", "OUT_STD", "Output Transformation")]) + "\n"
            + "\n".join(conns("IN_STD", "EXP_STD", [(c, c) for c in ["CUSTOMER_ID", "EMAIL", "SEGMENT", "COUNTRY"]], "Input Transformation", "Expression")
                        + conns("EXP_STD", "OUT_STD", [(c, c) for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY"]], "Expression", "Output Transformation"))
            + '\n        </MAPPLET>')
    u = [c[0] for c in COLS["CUSTOMER_UPDATES"]]
    exp_hash = [tfield(c, "INPUT/OUTPUT") for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY"]] + [
        tfield("o_SRC_HASH", "OUTPUT", p=32, expr="MD5(o_EMAIL || '|' || o_SEGMENT || '|' || o_COUNTRY)")]
    lkp = [tfield("IN_CUSTOMER_ID", "INPUT", "decimal", 10, 0), tfield("CUSTOMER_ID", "LOOKUP", "decimal", 10, 0),
           tfield("CUSTOMER_SK", "LOOKUP/OUTPUT", "decimal", 10, 0), tfield("DIM_HASH", "LOOKUP/OUTPUT", "string", 32)]
    lkp_attrs = [("Lookup Sql Override", "SELECT DIM_CUSTOMER.CUSTOMER_SK AS CUSTOMER_SK, DIM_CUSTOMER.CUSTOMER_ID AS CUSTOMER_ID, "
                                         "MD5(DIM_CUSTOMER.EMAIL || '|' || DIM_CUSTOMER.SEGMENT || '|' || DIM_CUSTOMER.COUNTRY) AS DIM_HASH "
                                         "FROM DIM_CUSTOMER WHERE DIM_CUSTOMER.CURRENT_FLAG = 'Y'"),
                 ("Lookup table name", "DIM_CUSTOMER"), ("Lookup condition", "CUSTOMER_ID = IN_CUSTOMER_ID"),
                 ("Lookup policy on multiple match", "Report Error"), ("Lookup caching enabled", "YES"), ("Dynamic Lookup Cache", "NO")]
    flag = [tfield(c, "INPUT/OUTPUT") for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_SRC_HASH", "CUSTOMER_SK", "DIM_HASH"]] + [
        tfield("o_CHANGE_FLAG", "OUTPUT", p=10, expr="IIF(ISNULL(CUSTOMER_SK), 'NEW', IIF(o_SRC_HASH != DIM_HASH, 'CHANGED', 'NOCHANGE'))"),
        tfield("o_LOAD_DATE", "OUTPUT", "date/time", 29, 9, expr="TO_DATE($$LOAD_DATE, 'YYYY-MM-DD')"),
        tfield("o_EXPIRE_DATE", "OUTPUT", "date/time", 29, 9, expr="ADD_TO_DATE(TO_DATE($$LOAD_DATE, 'YYYY-MM-DD'), 'DD', -1)")]
    rcols = ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "CUSTOMER_SK", "o_CHANGE_FLAG", "o_LOAD_DATE", "o_EXPIRE_DATE"]
    groups = [("INPUT", "INPUT", ""), ("NEW", "OUTPUT", "o_CHANGE_FLAG = 'NEW'"), ("CHANGED", "OUTPUT", "o_CHANGE_FLAG = 'CHANGED'"), ("DEFAULT1", "OUTPUT/DEFAULT", "")]
    gxml = "\n".join(f'            <GROUP {a(DESCRIPTION="", EXPRESSION=e, NAME=g, ORDER=i + 1, TYPE=t)}/>' for i, (g, t, e) in enumerate(groups))
    rfields = [tfield(c if gi == 0 else f"{c}{gi}", "INPUT" if gi == 0 else "OUTPUT", GROUP=g, **({} if gi == 0 else {"REF_FIELD": c}))
               for gi, (g, _, _) in enumerate(groups) for c in rcols]
    upd = lambda name, expr_, cols: trans(name, "Update Strategy", [tfield(c, "INPUT/OUTPUT") for c in cols],
                                          [("Update Strategy Expression", expr_), ("Forward Rejected Rows", "YES")])
    exp_ins = [tfield(c, "INPUT/OUTPUT") for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_LOAD_DATE"]] + [
        tfield("NEXTVAL", "INPUT", "bigint", 19, 0), tfield("o_CUSTOMER_SK", "OUTPUT", "bigint", 19, 0, expr="NEXTVAL"),
        tfield("o_VALID_TO", "OUTPUT", "date/time", 29, 9, expr="TO_DATE('9999-12-31', 'YYYY-MM-DD')"),
        tfield("o_CURRENT_FLAG", "OUTPUT", p=1, expr="'Y'")]
    seq = [tfield("NEXTVAL", "OUTPUT", "bigint", 19, 0), tfield("CURRVAL", "OUTPUT", "bigint", 19, 0)]
    seq_attrs = [("Start Value", "1"), ("Increment By", "1"), ("Current Value", "1001"), ("Cycle", "NO"), ("Reset", "NO"), ("Number of Cached Values", "0")]
    dimf = COLS["DIM_CUSTOMER"]
    T = [sq("SQ_CUSTOMER_UPDATES", "CUSTOMER_UPDATES", source_filter="CUSTOMER_UPDATES.EXTRACT_DATE = TO_DATE('$$LOAD_DATE', 'YYYY-MM-DD')"),
         trans("EXP_HASH", "Expression", exp_hash, desc="Change-detection hash over the tracked SCD2 attributes."),
         trans("LKP_DIM_CURRENT", "Lookup Procedure", lkp, lkp_attrs, desc="Current dimension row per customer (SQL override computes the hash)."),
         trans("EXP_CHANGE_FLAG", "Expression", flag),
         trans("RTR_CHANGE", "Router", rfields, extra_xml=gxml, desc="NEW and CHANGED rows; NOCHANGE falls into DEFAULT1 (not connected = dropped)."),
         trans("SEQ_CUSTOMER_SK", "Sequence", seq, seq_attrs, desc="Shared surrogate-key sequence, persisted Current Value = 1001."),
         trans("EXP_NEW_VERSION", "Expression", exp_ins), trans("EXP_NEW_VERSION_CHG", "Expression", exp_ins),
         upd("UPD_INSERT_NEW", "DD_INSERT", ["o_CUSTOMER_SK", "o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_LOAD_DATE", "o_VALID_TO", "o_CURRENT_FLAG"]),
         upd("UPD_INSERT_CHANGED", "DD_INSERT", ["o_CUSTOMER_SK", "o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_LOAD_DATE", "o_VALID_TO", "o_CURRENT_FLAG"]),
         trans("UPD_EXPIRE_OLD", "Update Strategy", [tfield("CUSTOMER_SK2", "INPUT/OUTPUT", "decimal", 10, 0), tfield("o_EXPIRE_DATE2", "INPUT/OUTPUT", "date/time", 29, 9),
                                                     tfield("o_CURRENT_FLAG_N", "OUTPUT", p=1, expr="'N'")],
               [("Update Strategy Expression", "DD_UPDATE"), ("Forward Rejected Rows", "YES")], desc="Expire the previous current version (update keyed on CUSTOMER_SK).")]
    I = [inst("CUSTOMER_UPDATES", "CUSTOMER_UPDATES", "Source Definition", "SOURCE"),
         inst("SQ_CUSTOMER_UPDATES", "SQ_CUSTOMER_UPDATES", "Source Qualifier", assoc="CUSTOMER_UPDATES"),
         inst("mplt_STD_CUSTOMER", "mplt_STD_CUSTOMER", "Mapplet", "MAPPLET")]
    I += [inst(n, n, t) for n, t in [("EXP_HASH", "Expression"), ("LKP_DIM_CURRENT", "Lookup Procedure"), ("EXP_CHANGE_FLAG", "Expression"), ("RTR_CHANGE", "Router"),
                                      ("SEQ_CUSTOMER_SK", "Sequence"), ("EXP_NEW_VERSION", "Expression"), ("EXP_NEW_VERSION_CHG", "Expression"),
                                      ("UPD_INSERT_NEW", "Update Strategy"), ("UPD_INSERT_CHANGED", "Update Strategy"), ("UPD_EXPIRE_OLD", "Update Strategy")]]
    I += [inst("DIM_CUSTOMER_INS_NEW", "DIM_CUSTOMER", "Target Definition", "TARGET"), inst("DIM_CUSTOMER_INS_CHG", "DIM_CUSTOMER", "Target Definition", "TARGET"),
          inst("DIM_CUSTOMER_UPD", "DIM_CUSTOMER", "Target Definition", "TARGET")]
    ins_map = [("o_CUSTOMER_SK", "CUSTOMER_SK"), ("o_CUSTOMER_ID", "CUSTOMER_ID"), ("o_EMAIL", "EMAIL"), ("o_SEGMENT", "SEGMENT"), ("o_COUNTRY", "COUNTRY"),
               ("o_LOAD_DATE", "VALID_FROM"), ("o_VALID_TO", "VALID_TO"), ("o_CURRENT_FLAG", "CURRENT_FLAG")]
    C = (conns("CUSTOMER_UPDATES", "SQ_CUSTOMER_UPDATES", [(c, c) for c in u], "Source Definition", "Source Qualifier")
         + conns("SQ_CUSTOMER_UPDATES", "mplt_STD_CUSTOMER", [(c, c) for c in ["CUSTOMER_ID", "EMAIL", "SEGMENT", "COUNTRY"]], "Source Qualifier", "Mapplet")
         + conns("mplt_STD_CUSTOMER", "EXP_HASH", [(c, c) for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY"]], "Mapplet", "Expression")
         + [conn("EXP_HASH", "o_CUSTOMER_ID", "LKP_DIM_CURRENT", "IN_CUSTOMER_ID", "Expression", "Lookup Procedure")]
         + conns("EXP_HASH", "EXP_CHANGE_FLAG", [(c, c) for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_SRC_HASH"]], "Expression", "Expression")
         + conns("LKP_DIM_CURRENT", "EXP_CHANGE_FLAG", [("CUSTOMER_SK", "CUSTOMER_SK"), ("DIM_HASH", "DIM_HASH")], "Lookup Procedure", "Expression")
         + conns("EXP_CHANGE_FLAG", "RTR_CHANGE", [(c, c) for c in rcols], "Expression", "Router")
         + conns("RTR_CHANGE", "EXP_NEW_VERSION", [(f"{c}1", c) for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_LOAD_DATE"]], "Router", "Expression")
         + conns("RTR_CHANGE", "EXP_NEW_VERSION_CHG", [(f"{c}2", c) for c in ["o_CUSTOMER_ID", "o_EMAIL", "o_SEGMENT", "o_COUNTRY", "o_LOAD_DATE"]], "Router", "Expression")
         + [conn("SEQ_CUSTOMER_SK", "NEXTVAL", "EXP_NEW_VERSION", "NEXTVAL", "Sequence", "Expression"),
            conn("SEQ_CUSTOMER_SK", "NEXTVAL", "EXP_NEW_VERSION_CHG", "NEXTVAL", "Sequence", "Expression")]
         + conns("RTR_CHANGE", "UPD_EXPIRE_OLD", [("CUSTOMER_SK2", "CUSTOMER_SK2"), ("o_EXPIRE_DATE2", "o_EXPIRE_DATE2")], "Router", "Update Strategy")
         + conns("EXP_NEW_VERSION", "UPD_INSERT_NEW", [(f, f) for f, _ in ins_map], "Expression", "Update Strategy")
         + conns("EXP_NEW_VERSION_CHG", "UPD_INSERT_CHANGED", [(f, f) for f, _ in ins_map], "Expression", "Update Strategy")
         + conns("UPD_INSERT_NEW", "DIM_CUSTOMER_INS_NEW", ins_map, "Update Strategy", "Target Definition")
         + conns("UPD_INSERT_CHANGED", "DIM_CUSTOMER_INS_CHG", ins_map, "Update Strategy", "Target Definition")
         + conns("UPD_EXPIRE_OLD", "DIM_CUSTOMER_UPD", [("CUSTOMER_SK2", "CUSTOMER_SK"), ("o_EXPIRE_DATE2", "VALID_TO"), ("o_CURRENT_FLAG_N", "CURRENT_FLAG")], "Update Strategy", "Target Definition"))
    var = ('            <MAPPINGVARIABLE ' + a(AGGFUNCTION="MAX", DATATYPE="string", DEFAULTVALUE="2026-10-31", DESCRIPTION="Business load date (YYYY-MM-DD), set in the parameter file.",
                                               ISEXPRESSIONVARIABLE="NO", ISPARAM="YES", NAME="$$LOAD_DATE", PRECISION="10", SCALE="0", USERDEFINED="YES") + '/>')
    wf = session("m_DIM_CUSTOMER_SCD2", "s_m_DIM_CUSTOMER_SCD2",
                 [("Treat source rows as", "Data driven"), ("Parameter Filename", "$PMRootDir/params/dim_customer.par"),
                  ("Pre SQL", "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'"), ("Post SQL", "BEGIN DBMS_STATS.GATHER_TABLE_STATS('DWH','DIM_CUSTOMER'); END;"),
                  ("On Failure Email", "dwh-oncall@retail.example"), ("Commit Interval", "10000"), ("Truncate target table option", "NO")],
                 ["DIM_CUSTOMER_INS_NEW", "DIM_CUSTOMER_INS_CHG", "DIM_CUSTOMER_UPD"])
    write("05_very_complex_dim_customer_scd2", "m_DIM_CUSTOMER_SCD2",
          doc([source("CUSTOMER_UPDATES", db="STAGING", owner="STG"), source("DIM_CUSTOMER", db="DWH", owner="DWH"), target("DIM_CUSTOMER", COLS["DIM_CUSTOMER"])],
              "m_DIM_CUSTOMER_SCD2", "SCD Type 2 load of DIM_CUSTOMER from the daily customer change feed.", T, I, C, variables=var, workflow=wf, mapplets=mplt))


if __name__ == "__main__":
    m01(); m02(); m03(); m04(); m05()
