


# Create your views here.
from django.shortcuts import render, redirect
from django.http import JsonResponse
from .forms import UploadFileForm
from .tasks import process_uploaded_files
import logging
from django.shortcuts import render
from django.core.cache import cache
import oracledb
import tempfile
import os
import logging
import cx_Oracle
import datetime
from django.db import connection
from .tasks import upload_transactions_to_db
from datetime import datetime
from django.http import HttpResponse


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)



# def upload_files(request):
#     if request.method == 'POST':
#         form = UploadFileForm(request.POST, request.FILES)
#         if form.is_valid():
#             bank_name = form.cleaned_data['bank_name']
#             merchant_name = form.cleaned_data['merchant_name']
#             transaction_type = form.cleaned_data['transaction_type']
#             files = request.FILES.getlist('file')
            
#             # Prepare a list to hold the file contents and their formats
#             file_contents = []
#             file_formats = []  # This will hold the format of each file

#             for file in files:
#                 if file.size == 0:  # Check for empty files
#                     logger.error(f"File {file.name} is empty.")
#                     return JsonResponse({'message': f'File {file.name} is empty.'}, status=400)

#                 file_content = file.read()
#                 file_name = file.name
                
#                 logger.debug(f"Processing file: {file_name} with size {file.size} bytes.")

#                 # Determine file format based on extension
#                 if file_name.endswith('.xlsx') or file_name.endswith('.xls') or file_name.endswith('.ods'):
#                     file_format = 'excel'
#                 elif file_name.endswith('.csv'):
#                     file_format = 'csv'
#                 else:
#                     logger.error(f"Unsupported file extension for file: {file_name}")
#                     return JsonResponse({'message': 'Unsupported file extension. Only .xlsx, .xls, .ods, and .csv files are allowed.'}, status=400)

#                 # Store file content along with its name and format
#                 file_contents.append((file_content, file_name))  # Append the content and file name as a tuple
#                 file_formats.append(file_format)  # Append the format of the corresponding file

#             # Ensure that file_format is set
#             if not file_formats:
#                 logger.error("Could not determine file format for uploaded files.")
#                 return JsonResponse({'message': 'Unable to determine file format. Please upload .xlsx or .csv files or .xls files or .ods files.'}, status=400)

#             # Pass the full list of file contents, their names, bank name, transaction type, and formats to the task
#             process_uploaded_files.delay(file_contents, bank_name, transaction_type, file_formats, merchant_name)

#             return JsonResponse({'message': 'Files uploaded successfully. Processing started.'}, status=202)
#     else:
#         form = UploadFileForm()
    
#     return render(request, 'upload.html', {'form': form})


def upload_files(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            bank_name = form.cleaned_data['bank_name']
            merchant_name = form.cleaned_data['merchant_name']
            transaction_type = form.cleaned_data['transaction_type']
            files = request.FILES.getlist('file')
            
            temp_file_paths = []
            file_formats = []

            for file in files:
                if file.size == 0:
                    logger.error(f"File {file.name} is empty.")
                    return JsonResponse({'message': f'File {file.name} is empty.'}, status=400)

                # Create a temporary file with the correct extension
                file_extension = file.name.lower().split('.')[-1]
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_extension}')
                for chunk in file.chunks():
                    temp_file.write(chunk)
                temp_file.close()

                # Determine file format based on extension
                if file_extension in ['xlsx', 'xls', 'ods']:
                    file_formats.append('excel')
                else:
                    file_formats.append('csv')

                temp_file_paths.append(temp_file.name)

            # Pass the original file format list (file_formats) and other necessary info to Celery task
            process_uploaded_files.delay(temp_file_paths, bank_name, transaction_type, file_formats, merchant_name)
            return JsonResponse({'message': 'Processing started.'}, status=202)
    else:
        form = UploadFileForm()

    return render(request, 'upload.html', {'form': form})


# def upload_files(request):
#     if request.method == 'POST':
#         form = UploadFileForm(request.POST, request.FILES)
#         if form.is_valid():
#             bank_name = form.cleaned_data['bank_name']
#             merchant_name = form.cleaned_data['merchant_name']
#             transaction_type = form.cleaned_data['transaction_type']
#             files = request.FILES.getlist('file')
            
#             temp_file_paths = []
#             file_formats = []

#             for file in files:
#                 if file.size == 0:
#                     logger.error(f"File {file.name} is empty.")
#                     return JsonResponse({'message': f'File {file.name} is empty.'}, status=400)

#                 temp_file = tempfile.NamedTemporaryFile(delete=False)
#                 for chunk in file.chunks():
#                     temp_file.write(chunk)
#                 temp_file.close()

#                 # file_formats.append('excel' if file.name.endswith(('.xlsx', '.xls', '.ods')) else 'csv')
#                 file_formats.append('excel' if file.name.lower().endswith(('.xlsx', '.xls', '.ods')) else 'csv')
#                 temp_file_paths.append(temp_file.name)

#             process_uploaded_files.delay(temp_file_paths, bank_name, transaction_type, file_formats, merchant_name)
#             return JsonResponse({'message': 'Files uploaded successfully. Processing started.'}, status=202)
#     else:
#         form = UploadFileForm()
    
#     return render(request, 'upload.html', {'form': form})



def transaction_results_view(request):
    # Retrieve the results from the cache
    results = cache.get('latest_transaction_results')
    if results is None:
        logger.error("No cached results found.")
        results = {
            "total_successful": 0,
            "total_failed": 0,
        }
    else:
        logger.debug(f"Retrieved results from cache: {results}")

    context = {
        'total_successful': results['total_successful'],
        'total_failed': results['total_failed'],
    }

    return render(request, 'transaction_results.html', context)





###DB UPLOAD########################


def nget_db_upload(request):
    template = "success.html"
    form_template = "ngetdb.html"

    if request.method == 'POST':
        start_date = request.POST.get('start_date')
        end_date = request.POST.get('end_date')
        bank = request.POST.get('Bank')
        txn_type = request.POST.get('txn_type')

        try:
            # Updated to use '%d-%b-%Y' format (abbreviated month)
            start_date_oracle = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d-%b-%Y')
            end_date_oracle = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d-%b-%Y')

            if txn_type in ['SALE', 'REFUND', 'SOFT', 'IRDS']:
                task = upload_transactions_to_db.delay(start_date_oracle, end_date_oracle, bank, txn_type)
                context = {'msg': f"Data upload task for {txn_type} has started. Please check back later."}
                return render(request, template, context)

        except ValueError as e:
            context = {'msg': f"Error parsing dates: {e}"}
            return render(request, template, context)

    return render(request, form_template)




#######################################  UPDATE  MPR ACTUAL CREDIT DEBIT DATE ########################################################

def  Mpractual_Credit_Date(request):
    return render(request, "mprcredit.html", {})





# def nget_db_upload(request):
#     template = "success.html"
#     form_template = "ngetdb.html"

#     if request.method == 'POST':
#         start_date = request.POST.get('start_date')
#         end_date = request.POST.get('end_date')
#         bank = request.POST.get('Bank')
#         txn_type = request.POST.get('txn_type')

#         try:
#             start_date_oracle = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d-%m-%Y')
#             end_date_oracle = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d-%m-%Y')

#             if txn_type in ['SALE', 'REFUND', 'SOFT', 'IRDS']:
#                 task = upload_transactions_to_db.delay(start_date_oracle, end_date_oracle, bank, txn_type)
#                 context = {'msg': f"Data upload task for {txn_type} has started. Please check back later."}
#                 return render(request, template, context)

#         except ValueError as e:
#             context = {'msg': f"Error parsing dates: {e}"}
#             return render(request, template, context)

#     return render(request, form_template)




# def nget_db_upload(request):
#     template = "success.html"
#     form_template = "ngetdb.html"

#     if request.method == 'POST':
#         start_date = request.POST.get('start_date')
#         end_date = request.POST.get('end_date')
#         bank = request.POST.get('Bank')
#         txn_type = request.POST.get('txn_type')

#         try:
#             # Convert dates to Oracle DB format
#             start_date_oracle = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d-%m-%Y')
#             end_date_oracle = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d-%m-%Y')

#             if txn_type == 'SALE':
#                 # Trigger Celery task
#                 task = upload_transactions_to_db.delay(start_date_oracle, end_date_oracle, bank)
#                 context = {'msg': "Data upload task has started. Please check back later."}
#                 return render(request, template, context)

#         except ValueError as e:
#             # Handle date parsing errors
#             context = {'msg': f"Error parsing dates: {e}"}
#             return render(request, template, context)

#     return render(request, form_template)





# def transaction_list(request):
#     transactions = TransactionData.objects.all().order_by('-date')
#     paginator = Paginator(transactions, 20)  # Show 20 transactions per page
#     page_number = request.GET.get('page')
#     page_obj = paginator.get_page(page_number)
    
#     total_amount = TransactionData.objects.aggregate(Sum('amount'))['amount__sum']
    
#     context = {
#         'page_obj': page_obj,
#         'total_amount': total_amount,
#     }
#     return render(request, 'transaction_list.html', context)





