from django.shortcuts import render
from django.db import connections
import time


def index(request):
    return render(request, 'index.html')

def search(request):
    # Get the selected db and user's query
    # Return the query result or error message
    if request.GET.get('selectDatabase') == 'Redshift':
        db = 'redshift'
    else:
        db = 'default'

    if 'txt' in request.GET and request.GET['txt']:
        query = request.GET['txt']
        result = perform_query(db,query)
        return render(request,'index.html',result)


def perform_query(db,query):
    try:
        with connections[db].cursor() as cursor:
            start_time = time.time()
            try:
                cursor.execute(query)

                elapsed_time = round((time.time() - start_time) * 1000, 3)

                if cursor.description is None:
                    return cursor.rowcount

                columns = [col[0] for col in cursor.description]
                if cursor.rowcount > 100:
                    rows = list(list(row) for row in cursor.fetchmany(100))
                    message = 'We limit the number of rows displayed to 100.'
                    result = {'message':message, 'columns': columns, 'rows': rows, 'elapsed_time': elapsed_time}
                    return result
                else:
                    rows = list(list(row) for row in cursor.fetchall())
                    result = {'columns': columns, 'rows': rows, 'elapsed_time': elapsed_time}
                    return result


            except Exception as e:
                elapsed_time = round((time.time() - start_time) * 1000, 3)
                error = {}
                error['error'] = str(e.args)
                error['elapsed_time'] = elapsed_time
                return error

    except Exception as e:
        error = {}
        error['error'] = str(e.args)
        return error
