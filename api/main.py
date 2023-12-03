#!/usr/bin/env python

import os
import json
import redis
from flask import Flask, request, jsonify
from flask_cors import CORS , cross_origin
import pandas as pd
from linkextractor import columnas
import numpy as np
from scipy.spatial.distance import cityblock
import math


app = Flask(__name__)
CORS(app)

redis_conn = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

@app.route("/")
def index():
    return "Usage: http://<hostname>[:<prt>]/api/<url>"

#----------------------------------------------------------------
total = {}
valoresfinal = {}
peliculasp = {}
usuariosp = {}
df = pd.DataFrame()
midf = pd.DataFrame()
csv_path = '/shared_data/movie.csv'

@app.route('/api/csv', methods=['POST'])
def recibir_csv():
    global df
    global midf
    global usuariosp
    if request.method == 'POST':
        data = request.get_json()  
        theuser = data.get('user')  
        theuserx = int(theuser)

        #Pruebas de codigo---------------------
        csv_path = '/shared_data/movie25.csv'
        midf = pd.read_csv(csv_path , sep=";")
        #midf = midf.head(5000000)

        df_userselect = midf[midf['userId'] == theuserx]
        movie_ids_user1 = df_userselect['movieId'].tolist()
        rae = midf.query('movieId in @movie_ids_user1')


        rae['userId'] = rae['userId'].astype('int')
        rae['movieId'] = rae['movieId'].astype('int')
        #rae['rating'] = rae['rating'].astype('float32')

        #lsrae = readLargeFile(rae.head(100000)) 
        #rae = rae.head(100000)

        #Generamos un datframe por que es mas rapido y facil de manipular al hacer agrupaminto
        #pero puede consumir mas memoria
        consolidated_dfmi = rae.groupby(['userId', 'movieId'])['rating'].mean().unstack()

        #Generamos 5 dataframe , estos para cada instancia
        #instancia1 = consolidated_dfmi.head(20000) 
        instancia1 = pd.concat([consolidated_dfmi.query(f'userId == {theuserx}'), consolidated_dfmi.head(20000)])
        instancia1 = instancia1.loc[~instancia1.index.duplicated(keep='first')]

        #instancia2 = consolidated_dfmi.iloc[20000:50001]
        instancia2 = pd.concat([consolidated_dfmi.query(f'userId == {theuserx}'), consolidated_dfmi.iloc[20000:50001]])
        instancia2 = instancia2.loc[~instancia2.index.duplicated(keep='first')]

        #instancia3 = consolidated_dfmi.iloc[50000:70001]
        instancia3 = pd.concat([consolidated_dfmi.query(f'userId == {theuserx}'), consolidated_dfmi.iloc[50000:70001]])
        instancia3 = instancia3.loc[~instancia3.index.duplicated(keep='first')]

        #instancia4 = consolidated_dfmi.iloc[90000:100001]
        instancia4 = pd.concat([consolidated_dfmi.query(f'userId == {theuserx}'), consolidated_dfmi.iloc[90000:100001]])
        instancia4 = instancia4.loc[~instancia4.index.duplicated(keep='first')]

        #instancia5 = consolidated_dfmi.iloc[100000:]
        instancia5 = pd.concat([consolidated_dfmi.query(f'userId == {theuserx}'), consolidated_dfmi.iloc[100000:]])
        instancia5 = instancia5.loc[~instancia5.index.duplicated(keep='first')]

        columns1 = instancia1.columns
        values1 = instancia1.values
        columns2 = instancia2.columns
        values2 = instancia2.values
        columns3 = instancia3.columns
        values3 = instancia3.values
        columns4 = instancia4.columns
        values4 = instancia4.values
        columns5 = instancia5.columns
        values5 = instancia5.values

        lsrae1 = {user: {movie: rating for movie, rating in zip(columns1, row) if not pd.isna(rating)} for user, row in zip(instancia1.index, values1)}
        redis_conn.set('lsrae1', json.dumps(lsrae1))

        lsrae2 = {user: {movie: rating for movie, rating in zip(columns2, row) if not pd.isna(rating)} for user, row in zip(instancia2.index, values2)}
        redis_conn.set('lsrae2', json.dumps(lsrae2))

        lsrae3 = {user: {movie: rating for movie, rating in zip(columns3, row) if not pd.isna(rating)} for user, row in zip(instancia3.index, values3)}
        redis_conn.set('lsrae3', json.dumps(lsrae3))

        lsrae4 = {user: {movie: rating for movie, rating in zip(columns4, row) if not pd.isna(rating)} for user, row in zip(instancia4.index, values4)}
        redis_conn.set('lsrae4', json.dumps(lsrae4))

        lsrae5 = {user: {movie: rating for movie, rating in zip(columns5, row) if not pd.isna(rating)} for user, row in zip(instancia5.index, values5)}
        redis_conn.set('lsrae5', json.dumps(lsrae5))

        '''# Obtener las columnas y valores del DataFrame
        columns = consolidated_dfmi.columns
        values = consolidated_dfmi.values

        # Crear un diccionario a partir de los valores
        lsrae = {user: {movie: rating for movie, rating in zip(columns, row) if not pd.isna(rating)} for user, row in zip(consolidated_dfmi.index, values)}
        usuariosp = lsrae
        redis_conn.set('lsrae', json.dumps(lsrae))'''
        #--------------------------------------

        '''data = request.get_json()  
        nombre = data.get('obj')  
        df = pd.DataFrame(nombre)
        csv_path = '/shared_data/movie.csv'
        df.to_csv(csv_path, index=False)
        redis_conn.set('csv', json.dumps(nombre))'''
        return jsonify({"mensaje": "csv cargado correctamente a redis"})
    else:
        return jsonify({"mensaje": "Esta ruta solo acepta solicitudes POST"})


@app.route('/api/valor', methods=['POST'])
def recibir_datos():
    global valoresfinal , peliculasp ,usuariosp
    if request.method == 'POST':
        data = request.get_json()  

        col1 = data.get('col1')
        col2 = data.get('col2')
        col3 = data.get('col3')

        numero = data.get('numero')  
        numerox = int(numero)
        '''#csv_path = '/shared_data/movie.csv'
        csv_path = '/shared_data/movie25.csv'
        af = pd.read_csv(csv_path , sep=";")

        af = af.head(5000000)'''

        #peli = af

        '''peli = midf

        peli['userId'] = peli['userId'].astype('int')
        peli['movieId'] = peli['movieId'].astype('int')
        peli['rating'] = peli['rating'].astype('float32')'''


        def readLargeFile( data):
            #data = pd.read_csv(filename, delimiter=delim, header=None)
            data = data
            lst = {} # Dictionary
            j = 0
            for index, row in data.iterrows():
                #print(row[0], row[1], row[2])
                if j != row[0]:
                    j = row[0]
                    tmp = {row[1]:row[2]}
                    lst[row[0]] = tmp
                else:
                    tmp = lst.get(row[0])
                    tmp[row[1]] = row[2]
                    lst[row[0]] = tmp
            return lst
        
                # Manhattan distance
        def manhattanL(user1, user2):
            dist = 0.0
            count = 0
            for i in user2:
                if not (user1.get(i) is None):
                    x = user1[i]
                    y = user2[i]
                    dist += abs(x - y)
                    count += 1

            if count == 0:
                return 9999.99
            return dist
        
                # Init K-vector with correct value based on distance type
        def initVectDist(funName, N):
            if funName == 'euclidiana' or funName == 'manhattan' or funName == 'euclidianaL' or funName == 'manhattanL':
                ls = [99999] * N
            else:
                ls = [-1] * N

            lu = [None] * N
            return ls, lu


        # Keep the closest values, avoiding sort
        def keepClosest(funname, lstdist, lstuser, newdist, newuser, N):
            if funname == 'euclidiana' or funname == 'manhattan' or funname == 'euclidianaL' or funname == 'manhattanL':
                count = -1
                for i in lstdist:
                    count += 1
                    if newdist > i:
                        continue
                    lstdist.insert(count, newdist)
                    lstuser.insert(count, newuser)
                    break
            else:
                count = -1
                for i in lstdist:
                    count += 1
                    if newdist < i:
                        continue
                    lstdist.insert(count, newdist)
                    lstuser.insert(count, newuser)
                    break

            if len(lstdist) > N:
                lstdist.pop()
                lstuser.pop()
            return lstdist, lstuser
        
                # K-Nearest neighbour
        def knn_L(N, distancia, usuario, data):  # N numero de vecinos
            funName = distancia.__name__
            print('k-nn', funName)

            listDist, listName = initVectDist(funName, N)
            nsize = len(data)
            otherusers = range(0, nsize)
            vectoruser = data.get(usuario)
            claves_principales = list(data.keys())

            for i in claves_principales: #recorre de 0 a cantidad de datos del diicionario digamos 10
                tmpuser = i
                if tmpuser != usuario:
                    tmpvector = data.get(tmpuser)
                    if not (tmpvector is None):
                        tmpdist = distancia(vectoruser, tmpvector)
                        if tmpdist is not math.nan:
                            listDist, listName = keepClosest(funName, listDist, listName, tmpdist, tmpuser, N)

            return listDist, listName
        
        def topSuggestions(fullObj, k, items):
            rp = [-1]*items

            for i in fullObj:
                rating = fullObj.get(i)

                for j in range(0, items):
                    if rp[j] == -1 :
                        tmp = [i, rating[0], rating[1]]
                        rp.insert(j, tmp)
                        rp.pop()
                        break
                    else:
                        tval = rp[j]
                        if tval[1] < rating[0]:
                            tmp = [i, rating[0], rating[1]]
                            rp.insert(j, tmp)
                            rp.pop()
                            break

            return rp
        
        def recommendationL(usuario, distancia, N, items, minr, data):
            ldistK, luserK = knn_L(N, distancia, usuario, data)

            user = data.get(usuario)
            recom = [None] * N
            for i in range(0, N):
                recom[i] = data.get(luserK[i])
            # print('user preference:', user)

            lstRecomm = [-1] * items
            lstUser = [None] * items
            lstObj = [None] * items
            k = 0

            fullObjs = {}
            count = 0
            for i in recom:
                for j in i:
                    tmp = fullObjs.get(j)
                    if tmp is None:
                        fullObjs[j] = [i.get(j), luserK[count]]
                    else:
                        nval = i.get(j)
                        if nval > tmp[0]:
                            fullObjs[j] = [nval, luserK[count]]
                count += 1

            finallst = topSuggestions(fullObjs, count, items)
            return finallst
        
        


        '''df_userselect = peli[peli['userId'] == numerox]
        movie_ids_user1 = df_userselect['movieId'].tolist()
        rae = peli.query('movieId in @movie_ids_user1')'''

        '''rae['userId'] = rae['userId'].astype('int')
        rae['movieId'] = rae['movieId'].astype('int')
        rae['rating'] = rae['rating'].astype('float32')

        #lsrae = readLargeFile(rae.head(100000)) 
        #rae = rae.head(100000)
        #lsrae = rae.groupby('userId').apply(lambda x: dict(zip(x['movieId'], x['rating']))).to_dict()

        #-------Considerado aun mas veloz que el anterior e incluzo mas aun cuando hay mas datos-----
        consolidated_dfmi = rae.groupby(['userId', 'movieId'])['rating'].mean().unstack()
        # Obtener las columnas y valores del DataFrame
        columns = consolidated_dfmi.columns
        values = consolidated_dfmi.values

        # Crear un diccionario a partir de los valores
        lsrae = {user: {movie: rating for movie, rating in zip(columns, row) if not pd.isna(rating)} for user, row in zip(consolidated_dfmi.index, values)}'''

        lsrae_cached = redis_conn.get('lsrae1')
        lsrae = json.loads(lsrae_cached)

        datafinal = {int(k): {float(k2): v2 for k2, v2 in v.items()} for k, v in lsrae.items()}
        
        rfunc = manhattanL
        lista = recommendationL(numerox, rfunc, 10, 20, 3.0, datafinal)
  

        '''consolidated_dfmi = columnas(rae, col1, col2, col3)
        #consolidated_dfmi = consolidated_dfmi.head(300)
        #consolidated_dfmi = pd.concat([consolidated_dfmi.query(f'userId == {numerox}'), consolidated_dfmi.head(300)])
        consolidated_dfmi = pd.concat([consolidated_dfmi.query(f'userId == {numerox}'), consolidated_dfmi.head(20000)])
        consolidated_dfmi = consolidated_dfmi.loc[~consolidated_dfmi.index.duplicated(keep='first')]
        #consolidated_dfmi = consolidated_dfmi.fillna(0)


        def computeNearestNeighbor(dataframe, target_user, distance_metric=cityblock):
            distances = np.zeros(len(dataframe))
            target_row = dataframe.loc[target_user]  
            for i, (index, row) in enumerate(dataframe.iterrows()):
                if index == target_user:
                    continue  
                
                non_zero_values = (target_row != 0) & (row != 0)
                distance = distance_metric(target_row[non_zero_values].fillna(0), row[non_zero_values].fillna(0))
                distances[i] = distance
            
            sorted_indices = np.argsort(distances)
            sorted_distances = distances[sorted_indices]
            return list(zip(dataframe.index[sorted_indices], sorted_distances))
        

        target_user_id = numerox
        neighborsmi = computeNearestNeighbor(consolidated_dfmi, target_user_id)
        diccionario_resultante = dict(neighborsmi)
        valoresfinal = diccionario_resultante

        #pruebas
        cd2 = pd.DataFrame(neighborsmi)
        cd2.columns = ['Id_user', 'Distancias']

        primeros = cd2['Id_user'].unique().tolist()[:10] #lista de 10 primeros usuarios
        primeros.append(target_user_id)

        resul = peli.query('userId in @primeros') # filtra todos los datos donde aparecen esos 10 usuarios
        #newx = resul.query('rating == 5.0')['movieId'].drop_duplicates()
        recomendadatest = resul[~resul['movieId'].isin(movie_ids_user1)] #elimanmos peliculas que el usauario ya vio

        newx  = recomendadatest['movieId'].drop_duplicates() #eliminamos duplicados de peliculas
        #newx = resul['movieId'].drop_duplicates() 

        #covertmos los resultados de las peliculas a diccionario
        recomendada_df = pd.DataFrame({'movieId': newx})
        recomendada_df = recomendada_df.head(30)
        lista_tuplas = [(i, movie_id) for i, movie_id in enumerate(recomendada_df['movieId'])]

        datarecomend = dict(lista_tuplas)
        peliculasp = datarecomend
'''
        tratado = [item for item in lista if item != -1]
        peliculasp = {i: item[0] for i, item in enumerate(tratado, start=1)}
       
        
    

        redis_conn.set('valoresfinal', json.dumps(valoresfinal))
        redis_conn.set('peliculas', json.dumps(peliculasp))

        #return jsonify(valoresfinal)
        return jsonify("exitoso")
    else:
        return jsonify({"mensaje": "Esta ruta solo acepta solicitudes POST"})
#----------------------------------------------------------------



@app.route('/api/valor', methods=['GET'])
def get_users():
    # Intenta recuperar datos desde Redis
    cached_data = redis_conn.get('valoresfinal') 
    if cached_data:
        return jsonify(json.loads(cached_data))
    else:
        return jsonify({"mensaje": "No hay valores finales almacenados en Redis"})

@app.route('/api/peliculas', methods=['GET'])
def get_peliculas():
    peliculas_cached = redis_conn.get('peliculas') 
    if peliculas_cached:
        return jsonify(json.loads(peliculas_cached))
    else:
        return jsonify({"mensaje": "No hay peliculas finales almacenados en Redis"})


  
    

@app.route('/api/csv', methods=['GET'])
def get_csv():
    csv_cached = redis_conn.get('lsrae') 
    if csv_cached:
        csvx = json.loads(csv_cached)
        return jsonify(csvx)
    else:
        return jsonify({"mensaje": "No hay valores finales almacenados en Redis"})


app.run(host="0.0.0.0")
