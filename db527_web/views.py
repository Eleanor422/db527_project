from django.shortcuts import render
from django.db import connections
import time


# SELECT * FROM PG_TABLE_DEF
# SHOW TABLES

def index(request):
    return render(request, 'index.html')

def search(request):
    if request.GET.get('selectDatabase') == 'Redshift':
        db = 'redshift'
    else:
        db = 'default'

    if 'txt' in request.GET and request.GET['txt']:
        query = request.GET['txt']
        columns, rows, elapsed_time = perform_query(db,query)
        return render(request,'index.html',{'columns':columns,'rows':rows, 'elapsed_time':elapsed_time})


def perform_query(db,query):
    try:
        with connections[db].cursor() as cursor:

            start_time = time.time()
            cursor.execute(query)

            if cursor.description is None:
                return cursor.rowcount
            if cursor.rowcount > 20:
                rows = list(list(row) for row in cursor.fetchmany(20))
            else:
                rows = list(list(row) for row in cursor.fetchall())

            columns = [col[0] for col in cursor.description]

            elapsed_time = round((time.time() - start_time)*1000,3)

            return columns, rows, elapsed_time

    except Exception as e:
        return e
