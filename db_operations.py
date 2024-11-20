
import oracledb
import os
import logging

logger = logging.getLogger(__name__)

# # Environment variables for DB credentials
# DB_USER = os.getenv('DB_USER', 'default_user')
# DB_PASSWORD = os.getenv('DB_PASSWORD', 'default_password')
# DB_DSN = os.getenv('DB_DSN', 'default_dsn')


def get_transaction_data_in_chunks(start_date, end_date, bank, txn_type, chunk_size=50000):
    connection = None
    cursor = None

    try:
        connection = oracledb.connect(user='PGACT7', password='Nov2024', dsn='10.78.14.42:1725/rptdb_srv.cris.org.in')
        cursor = connection.cursor()
        cursor.arraysize = chunk_size

        sql_queries = {
            'SALE': """
                SELECT To_CHAR(B.ENTITY_ID) as "RESERVATION_ID",TO_CHAR(B.PAYMENT_DATE, 'DD-MON-YYYY') AS "BOOKING_DATE",
                            TO_CHAR(B.BANK_TRANSACTION_NUMBER) AS "BANK_TRANSACTION_NUMBER", B.AMOUNT AS "BOOKING_AMOUNT", B.BANK_ID AS "BANK_ID", DECODE (BANK_ID, '1', 'SBI NB WEB', '3','SBI DR WEB','4',
                            'ICICI PG WEB','5','INDIAN OVERSEAS DR WEB','9','PNB DR WEB','10','SBIA NB WEB','15','INDIAN DR WEB','16','UBI DR WEB',
                            '17','CITI PG WEB','19','BOI DR WEB','21','HDFC PG WEB','22','FEDERAL NB WEB','25','ANDHRA DR WEB','26','CANARA DR WEB','27','AMEX PG WEB','28','UBI NB WEB',
                            '29','INDIAN NB WEB','30','AXIS PG WEB','31','ANDHRA NB WEB','32','CITI DR WEB','34','PNB NB WEB','35','ALLAHABAD NB WEB','36','HDFC NB WEB','37','BOB NB WEB',
                            '38','VIJAYA NB WEB','39','AXIS NB WEB','40','KARUR VYSYA NB WEB','41','ICICI DR WEB','42','KARNATAKA NB WEB','43','OBC NB WEB','44','ICICI NB WEB','45','INDUSIND NB WEB',
                            '46','KOTAK MAHINDRA NB WEB','47','ING VYSYA (NOW KOTAK) NB WEB','48','BOI NB WEB','49','CITI EMI WEB','50','CBI NB WEB','51','IMPS NB WEB','52','IDBI NB WEB', 
                            '53','BOM NB WEB','54','SYNDICATE NB WEB','56','CORPORATION NB WEB','57','HDFC DR WEB','58','KOTAK PG WEB','59','IRCTC UBI WALLET WEB','60','YES NB WEB', 
                            '63','PAYTM WALLET WEB', '64','NEPAL SBI NB WEB','66','AXIS DR WEB','67','SOUTH INDIAN NB WEB','69','CBI DR WEB','113','IRCTC Ipay MPP WEB','115','ICICI PG WEB',
                            '118','IRCTC Ipay MPP APP','120','ICICI PG APP','122','HDFC MPP APP','124','AMEX PG APP','125','INDUSIND BANK MPP APP','129','KOTAK BANK PG WEB') As "BANK_NAME",'BOOKING' AS "PRODUCT_TYPE",
                            B.BANK_EXTRA_FIELD1 AS "BANK_EXTRA_REF",B.ENTITY_CODE AS "ENTITY",(TO_TIMESTAMP_TZ (CURRENT_TIMESTAMP, 'DD-MON-RR HH.MI.SSXFF PM TZH:TZM')) AS "File_upload_Date"
                            FROM TRANSACTION_DB.ET_PAYMENT_CASH B
                            WHERE B.PAYMENT_DATE BETWEEN TO_DATE(:1,'dd-mm-yyyy') AND TO_DATE(:2,'dd-mm-yyyy')+0.99999
                            AND B.TRANSACTION_STATUS IN (13,15)
                            AND B.ENTITY_CODE NOT IN ('4','17','18')
                            AND B.BANK_ID = :3
                            ORDER BY TO_CHAR(B.PAYMENT_DATE, 'DD-MON-YYYY')
            """,
            'REFUND': """
                SELECT TO_CHAR(A.RESERVATION_ID) as "RESERVATION_ID",TO_CHAR(A.TRANSACTION_DATE, 'DD-MON-YYYY') AS "TRANSACTION_DATE",TO_CHAR(A.BANK_TRANSACTION_ID) as "BANK TRANSACTION ID",
                            TO_CHAR(A.BANK_REFUND_TXN_ID) as "BANK REFUND TXN ID", A.REFUND_AMOUNT AS "REFUNDAMOUNT",A.REFUND_STATUS as "REFUND STATUS",
                            DECODE (A.BANK_ID, '1', 'SBI NB WEB', '3','SBI DR WEB','4','ICICI PG WEB','5','INDIAN OVERSEAS DR WEB','9','PNB DR WEB','10','SBIA NB WEB','15','INDIAN DR WEB','16','UBI DR WEB',
                            '17','CITI PG WEB','19','BOI DR WEB','21','HDFC PG WEB','22','FEDERAL NB WEB','25','ANDHRA DR WEB','26','CANARA DR WEB','27','AMEX PG WEB','28','UBI NB WEB',
                            '29','INDIAN NB WEB','30','AXIS PG WEB','31','ANDHRA NB WEB','32','CITI DR WEB','34','PNB NB WEB','35','ALLAHABAD NB WEB','36','HDFC NB WEB','37','BOB NB WEB',
                            '38','VIJAYA NB WEB','39','AXIS NB WEB','40','KARUR VYSYA NB WEB','41','ICICI DR WEB','42','KARNATAKA NB WEB','43','OBC NB WEB','44','ICICI NB WEB','45','INDUSIND NB WEB',
                            '46','KOTAK MAHINDRA NB WEB','47','ING VYSYA (NOW KOTAK) NB WEB','48','BOI NB WEB','49','CITI EMI WEB','50','CBI NB WEB','51','IMPS NB WEB','52','IDBI NB WEB', 
                            '53','BOM NB WEB','54','SYNDICATE NB WEB','56','CORPORATION NB WEB','57','HDFC DR WEB','58','KOTAK PG WEB','59','IRCTC UBI WALLET WEB','60','YES NB WEB', 
                            '63','PAYTM WALLET WEB', '64','NEPAL SBI NB WEB','66','AXIS DR WEB','67','SOUTH INDIAN NB WEB','69','CBI DR WEB','113','IRCTC Ipay MPP WEB','115','ICICI PG WEB',
                            '118','IRCTC Ipay MPP APP','120','ICICI PG APP','122','HDFC MPP APP','124','AMEX PG APP','125','INDUSIND BANK MPP APP','129','KOTAK BANK PG WEB') As "BANK_NAME",
                            TO_CHAR(A.ACTUAL_REFUND_DATE, 'DD-MON-YYYY') AS "ACTUAL REFUND DATE",A.BANK_ID, A.CANCELLATION_ID as "CANCELLATION_ID", '' AS "OTP FLAG" ,'REFUND' AS "PRODUCT_TYPE",
                            B.BANK_EXTRA_FIELD1 AS "BANK_EXTRA_REF",(TO_TIMESTAMP_TZ (CURRENT_TIMESTAMP, 'DD-MON-RR HH.MI.SSXFF PM TZH:TZM')) AS "File_upload_Date"
                            FROM TRANSACTION_DB.ET_REFUND A,TRANSACTION_DB.ET_PAYMENT_CASH B
                            WHERE A.RESERVATION_ID=B.ENTITY_ID
                            AND A.TRANSACTION_DATE BETWEEN TO_DATE(:1,'DD-MON-YYYY') AND TO_DATE(:2,'DD-MON-YYYY')+0.99999
                            AND A.BANK_ID = :3
                            AND A.REFUND_STATUS <>'3'
                            ORDER BY A.TRANSACTION_DATE
            """,
            # 'SOFT': """
            #     SELECT TO_CHAR(ENTITY_ID) As "RESERVATION_ID",TO_CHAR(PAYMENT_DATE, 'DD-MON-YYYY') AS "BOOKING_DATE", 
            #                 TO_CHAR(BANK_TRANSACTION_NUMBER) as "BANK_TRANSACTION NUBER", Amount as "BOOKING AMOUNT", BANK_ID,
            #                 DECODE (BANK_ID, '1', 'SBI NB WEB', '3','SBI DR WEB','4','ICICI PG WEB','5','INDIAN OVERSEAS DR WEB','9','PNB DR WEB','10','SBIA NB WEB','15','INDIAN DR WEB','16','UBI DR WEB',
            #                 '17','CITI PG WEB','19','BOI DR WEB','21','HDFC PG WEB','22','FEDERAL NB WEB','25','ANDHRA DR WEB','26','CANARA DR WEB','27','AMEX PG WEB','28','UBI NB WEB',
            #                 '29','INDIAN NB WEB','30','AXIS PG WEB','31','ANDHRA NB WEB','32','CITI DR WEB','34','PNB NB WEB','35','ALLAHABAD NB WEB','36','HDFC NB WEB','37','BOB NB WEB',
            #                 '38','VIJAYA NB WEB','39','AXIS NB WEB','40','KARUR VYSYA NB WEB','41','ICICI DR WEB','42','KARNATAKA NB WEB','43','OBC NB WEB','44','ICICI NB WEB','45','INDUSIND NB WEB',
            #                 '46','KOTAK MAHINDRA NB WEB','47','ING VYSYA (NOW KOTAK) NB WEB','48','BOI NB WEB','49','CITI EMI WEB','50','CBI NB WEB','51','IMPS NB WEB','52','IDBI NB WEB', 
            #                 '53','BOM NB WEB','54','SYNDICATE NB WEB','56','CORPORATION NB WEB','57','HDFC DR WEB','58','KOTAK PG WEB','59','IRCTC UBI WALLET WEB','60','YES NB WEB', 
            #                 '63','PAYTM WALLET WEB', '64','NEPAL SBI NB WEB','66','AXIS DR WEB','67','SOUTH INDIAN NB WEB','69','CBI DR WEB','113','IRCTC Ipay MPP WEB','115','ICICI PG WEB',
            #                 '118','IRCTC Ipay MPP APP','120','ICICI PG APP','122','HDFC MPP APP','124','AMEX PG APP','125','INDUSIND BANK MPP APP','129','KOTAK BANK PG WEB') As "Bank_Name",'SOFT' AS "PRODUCT_TYPE",
            #                 BANK_EXTRA_FIELD1 AS "BANK_EXTRA_REF",(TO_TIMESTAMP_TZ (CURRENT_TIMESTAMP, 'DD-MON-RR HH.MI.SSXFF PM TZH:TZM')) AS "File_upload_Date",:1
            #                 FROM TRANSACTION_DB.ET_PAYMENT_CASH
            #                 WHERE PAYMENT_DATE BETWEEN TO_DATE(:2,'dd-mm-yyyy') AND TO_DATE (:3,'dd-mm-yyyy')+0.99999
            #                 AND TRANSACTION_STATUS IN(13 , 15)
            #                 AND ENTITY_CODE =4
            #                 AND BANK_ID = :4
            #                 ORDER BY TO_CHAR(PAYMENT_DATE, 'DD-MON-YYYY')
            # """,
            # 'IRDS': """
            #     SELECT TO_CHAR(ENTITY_ID) As "RESERVATION_ID",TO_CHAR(PAYMENT_DATE,'DD-MON-YYYY') AS "BOOKING_DATE", TO_CHAR(BANK_TRANSACTION_NUMBER) as "BANK_TRANSACTION NUBER", Amount as "BOOKING AMOUNT",BANK_ID,
            #                 DECODE (BANK_ID, '1', 'SBI NB WEB', '3','SBI DR WEB','4','ICICI PG WEB','5','INDIAN OVERSEAS DR WEB','9','PNB DR WEB','10','SBIA NB WEB','15','INDIAN DR WEB',
            #                 '16','UBI DR WEB','17','CITI PG WEB','19','BOI DR WEB','21','HDFC PG WEB','22','FEDERAL NB WEB','25','ANDHRA DR WEB','26','CANARA DR WEB','27','AMEX PG WEB','28','UBI NB WEB',
            #                 '29','INDIAN NB WEB','30','AXIS PG WEB','31','ANDHRA NB WEB','32','CITI DR WEB','34','PNB NB WEB','35','ALLAHABAD NB WEB','36','HDFC NB WEB','37','BOB NB WEB','38','VIJAYA NB WEB',
            #                 '39','AXIS NB WEB','40','KARUR VYSYA NB WEB','41','ICICI DR WEB','42','KARNATAKA NB WEB','43','OBC NB WEB','44','ICICI NB WEB','45','INDUSIND NB WEB','46','KOTAK MAHINDRA NB WEB',
            #                 '47','ING VYSYA (NOW KOTAK) NB WEB','48','BOI NB WEB','49','CITI EMI WEB','50','CBI NB WEB','51','IMPS NB WEB','52','IDBI NB WEB','53','BOM NB WEB','54','SYNDICATE NB WEB',
            #                 '56','CORPORATION NB WEB','57','HDFC DR WEB','58','KOTAK PG WEB','59','IRCTC UBI WALLET WEB','60','YES NB WEB','63','PAYTM WALLET WEB', '64','NEPAL SBI NB WEB','66','AXIS DR WEB',
            #                 '67','SOUTH INDIAN NB WEB','69','CBI DR WEB','113','IRCTC Ipay MPP WEB','115','ICICI PG WEB', '118','IRCTC Ipay MPP APP','120','ICICI PG APP','122','HDFC MPP APP','124','AMEX PG APP',
            #                 '125','INDUSIND BANK MPP APP','129','KOTAK BANK PG WEB') As "Bank_Name",'IRDS' AS "PRODUCT_TYPE",BANK_EXTRA_FIELD1 AS "BANK_EXTRA_REF",(TO_TIMESTAMP_TZ (CURRENT_TIMESTAMP, 'DD-MON-RR HH.MI.SSXFF PM TZH:TZM')) AS "File_upload_Date",:1
            #                 FROM TRANSACTION_DB.ET_PAYMENT_CASH 
            #                 WHERE PAYMENT_DATE BETWEEN TO_DATE(:2,'DD-MON-YYYY') AND TO_DATE(:3,'DD-MON-YYYY')+0.99999
            #                 AND TRANSACTION_STATUS IN('13','15')
            #                 AND ENTITY_CODE  IN ('17','18')
            #                 AND BANK_ID = :4
            #                 ORDER BY TO_CHAR(PAYMENT_DATE, 'DD-MON-YYYY')
            # """
        }

        sql = sql_queries.get(txn_type)
        if not sql:
            raise ValueError(f"No SQL query found for transaction type: {txn_type}")

        cursor.execute(sql, [start_date, end_date, bank])

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield rows

    except oracledb.DatabaseError as e:
        logger.error(f"Oracle Database Error: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


# def get_transaction_data_in_chunks(start_date, end_date, bank, chunk_size=50000):
#     connection = None
#     cursor = None

#     try:
#         connection = oracledb.connect(user='PGACT7', password='Nov2024', dsn='10.78.14.42:1725/rptdb_srv.cris.org.in')
#         cursor = connection.cursor()
#         cursor.arraysize = chunk_size

#         sql = """
#     SELECT TO_CHAR(B.ENTITY_ID) AS "RESERVATION_ID",
#            TO_CHAR(B.PAYMENT_DATE, 'DD-MON-YYYY') AS "BOOKING_DATE",
#            TO_CHAR(B.BANK_TRANSACTION_NUMBER) AS "BANK_TRANSACTION_NUMBER",
#            B.AMOUNT AS "BOOKING_AMOUNT",
#            B.BANK_ID AS "BANK_ID",
#            DECODE(B.BANK_ID, '1', 'SBI NB WEB', '3', 'SBI DR WEB', '4', 'ICICI PG WEB', 
#                   '5', 'INDIAN OVERSEAS DR WEB', '9', 'PNB DR WEB', '10', 'SBIA NB WEB',
#                   '15', 'INDIAN DR WEB', '16', 'UBI DR WEB', '17', 'CITI PG WEB', '19', 'BOI DR WEB',
#                   '21', 'HDFC PG WEB', '22', 'FEDERAL NB WEB', '25', 'ANDHRA DR WEB', '26', 'CANARA DR WEB',
#                   '27', 'AMEX PG WEB', '28', 'UBI NB WEB', '29', 'INDIAN NB WEB', '30', 'AXIS PG WEB',
#                   '31', 'ANDHRA NB WEB', '32', 'CITI DR WEB', '34', 'PNB NB WEB', '35', 'ALLAHABAD NB WEB',
#                   '36', 'HDFC NB WEB', '37', 'BOB NB WEB', '38', 'VIJAYA NB WEB', '39', 'AXIS NB WEB',
#                   '40', 'KARUR VYSYA NB WEB', '41', 'ICICI DR WEB', '42', 'KARNATAKA NB WEB', '43', 'OBC NB WEB',
#                   '44', 'ICICI NB WEB', '45', 'INDUSIND NB WEB', '46', 'KOTAK MAHINDRA NB WEB',
#                   '47', 'ING VYSYA (NOW KOTAK) NB WEB', '48', 'BOI NB WEB', '49', 'CITI EMI WEB',
#                   '50', 'CBI NB WEB', '51', 'IMPS NB WEB', '52', 'IDBI NB WEB', '53', 'BOM NB WEB',
#                   '54', 'SYNDICATE NB WEB', '56', 'CORPORATION NB WEB', '57', 'HDFC DR WEB',
#                   '58', 'KOTAK PG WEB', '59', 'IRCTC UBI WALLET WEB', '60', 'YES NB WEB', '63', 'PAYTM WALLET WEB',
#                   '64', 'NEPAL SBI NB WEB', '66', 'AXIS DR WEB', '67', 'SOUTH INDIAN NB WEB',
#                   '69', 'CBI DR WEB', '113', 'IRCTC Ipay MPP WEB', '115', 'ICICI PG WEB',
#                   '118', 'IRCTC Ipay MPP APP', '120', 'ICICI PG APP', '122', 'HDFC MPP APP',
#                   '124', 'AMEX PG APP', '125', 'INDUSIND BANK MPP APP', '129', 'KOTAK BANK PG WEB') AS "BANK_NAME",
#            'BOOKING' AS "PRODUCT_TYPE",
#            B.BANK_EXTRA_FIELD1 AS "BANK_EXTRA_REF",
#            B.ENTITY_CODE AS "ENTITY",
#            TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP, 'DD-MON-RR HH.MI.SSXFF PM TZH:TZM') AS "File_upload_Date"
#     FROM TRANSACTION_DB.ET_PAYMENT_CASH B
#     WHERE B.PAYMENT_DATE BETWEEN TO_DATE(:2, 'DD-MM-YYYY') AND TO_DATE(:3, 'DD-MM-YYYY') + 0.99999
#       AND B.TRANSACTION_STATUS IN (13, 15)
#       AND B.ENTITY_CODE NOT IN ('4', '17', '18')
#       AND B.BANK_ID = :4
#     ORDER BY TO_CHAR(B.PAYMENT_DATE, 'DD-MON-YYYY')
# """

#         # Execute the query with positional arguments
#         cursor.execute(sql, [start_date, end_date, bank])

#         while True:
#             rows = cursor.fetchmany(chunk_size)
#             if not rows:
#                 break
#             yield rows

#     except oracledb.DatabaseError as e:
#         logger.error(f"Oracle Database Error: {e}")
#         raise

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()

