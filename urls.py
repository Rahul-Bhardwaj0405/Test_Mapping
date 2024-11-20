from django.urls import path
from .views import  upload_files, transaction_results_view, nget_db_upload,  Mpractual_Credit_Date

urlpatterns = [
    path('mpr_upload/', upload_files, name='upload'),
    path('transaction-results/', transaction_results_view, name='transaction_results'),

    
# -------------NGET DB RECORD --------------------------------------------
    # path('dbrecord',ngetdb.nget_db_record, name='dbrecord'),
    # path('dbdownload',ngetdb.nget_db_download, name='dbdownload'),
    path('dbupload/', nget_db_upload, name='dbupload'), 
    # path('dbuploadrecord',ngetdb. nget_db_load, name='dbuploadrecord'),
    # path('airrecord',ngetdb.air_db_record, name='airrecord'),
    # path('airdownload',ngetdb.air_db_download, name='airdownload'),
    # path('airupload',ngetdb.air_db_upload, name='airupload'),


    # path('display/', display_data, name='display_data'),
    # path('task_status/', check_task_status, name='check_task_status'),


    
#--------------MERCHANT URL ------------------------------

    # path('raildb', merch.Raildb, name='raildb'),
    # path('railsaleload', merch.Railsale_load, name='railsaleload'),
    # path('mermpr', merch.mermpr, name='mermpr'),
    # path('mermprld', merch.mermpr_load, name='mermprld'),
    path('mprcredit/',  Mpractual_Credit_Date, name='mprcredit')
    # path('mermprsum', merch.Mpractual_Credit_Date, name='mermprsum'),
    # path('mermprupdate', merch.Mprupdate_Credit_Date, name='mermprupdate'),
    # path('merrecon', merch.mer_recon, name='merrecon'),
    # path('merdbupdate', merch.irctc_merdbupdate, name='merdbupdate'),
    # path('merbankstmatch', merch.irctc_merbstmatch, name='merbankstmatch'),
    # path('merirctcdb', merch.irctc_merdb, name='merirctcdb'),
    # path('merirctcmatch', merch.irctc_mermatch, name='merirctcmatch'),
    # path('merirctcnibf', merch.irctc_mernibf, name='merirctcnibf'),
    # path('merirctcnibxl', merch.irctc_mernibfxl, name='merirctcnibxl'),
    # path('merirctcnidb', merch.irctc_mernidb, name='merirctcnidb'),
    # path('merirctcnidbxl', merch.irctc_mernidbxl, name='merirctcnidbxl'),
    # path('merirctcexcessxl', merch.irctc_excesscrdrxl, name='merirctcexcessxl'),
    # path('lumsumld', merch.lumsum_Load_Crform, name='lumsumld'),
    # path('lumsumsum', merch.lusum_Credit_Date, name='lumsumsum'),
    # path('lumsumupdate', merch.lumsumupdate_Credit_Date, name='lumsumupdate'),
    # path('rlipayrecex', merch.Exceldw, name='rlipayrecex'),
    # path('rltxnxl', merch.Rly_txnxl,name='rltxnxl'),
    # path('rlipaymprxl', merch.Rly_ipaymprxl,name='rlipaymprxl'),
    # path('rlmatchxl', merch.Rly_matchxl,name='rlmatchxl'),
    # path('rldupbfxl', merch.Rly_dupbfxl,name='rldupbfxl'),
    # path('rldupdbxl', merch.Rly_dupdbxl,name='rldupdbxl'),
    # path('merresult', merch.result_mer_display, name='merresult'),
    # path('actualsummary', merch.Actual_Txn_Summary, name='actualsummary'),
    # path('actualtxn', merch.Actual_Txn, name='actualtxn'),
    # path('reconresult', merch.Recon_result, name='reconresult'),

]