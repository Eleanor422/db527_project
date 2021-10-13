from django.shortcuts import render
from django.db import connections
import pymysql
from django.http import HttpResponse


# SELECT * FROM PG_TABLE_DEF
# SHOW TABLES

def index(request):
    return render(request, 'index.html')

def search(request):
    print("reuest is", request)
    print("select db:", request.GET.get('selectDatabase'))

    if request.GET.get('selectDatabase') == 'Redshift':
        db = 'redshift'
    else:
        db = 'default'
    print("db is:", db)

    if 'txt' in request.GET and request.GET['txt']:
        query = request.GET['txt']
        print("query is",query)
        result = perform_query(db,query)
        print("result is:",result)
        return render(request,'index.html',{'output':result})


def perform_query(db,query):
    try:
        with connections[db].cursor() as cursor:
            cursor.execute(query)
            # columns = [col[0] for col in cursor.description]
            # return [
            #     dict(zip(columns, row))
            #     for row in cursor.fetchall()

            if cursor.description is None:
                return cursor.rowcount
            data = cursor.fetchall()
            lst = []
            heads = [column[0] for column in cursor.description]
            for rec in data:
                dic = {}
                for attr, head in zip(rec, heads):
                    dic[head] = attr
                lst.append(dic)
            return lst

    except Exception as e:
        print("error:", e)
        return e
