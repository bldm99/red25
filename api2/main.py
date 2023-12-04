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
from datatable import dt, f, by, g, join, sort, update, ifelse


app = Flask(__name__)
CORS(app)

redis_conn = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

@app.route("/")
def index():
    return "Usage: http://<hostname>[:<prt>]/apix/<url>"

#----------------------------------------------------------------
total = {}
valoresfinal = {}
peliculasp = {}
usuariosp = {}
df = pd.DataFrame()
midf = pd.DataFrame()
csv_path = '/shared_data/movie.csv'

@app.route('/apix/csv', methods=['POST'])
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
        #midf = pd.read_csv(csv_path , sep=";")
        midf = dt.fread(csv_path).to_pandas()
        #midf = midf.head(5000000)

        #Busacmos el usuario seleccionado en todos los 25M (puede mejorar)
        df_userselect = midf[midf['userId'] == theuserx]
        movie_ids_user1 = df_userselect['movieId'].tolist() #Todas las peliculas que vio en una lista
        #Buscamos todas esas peliculas en los 25M
        rae = midf.query('movieId in @movie_ids_user1')

        rae['userId'] = rae['userId'].astype('int')
        rae['movieId'] = rae['movieId'].astype('int')
        rae['rating'] = rae['rating'].astype('float32')

        #Obtenemos todos los usuarios que han vistos las mismas peliculas que nosotros
        users = rae['userId'].unique().tolist()

        #Dicvidiresmos todos estos usuarios en 5 partes
        num_parts = 5
        part_size = len(users) // num_parts

        user_parts = [users[i * part_size:(i + 1) * part_size] for i in range(num_parts)]
        user_parts[-1] += users[num_parts * part_size:]

        # Almacenamos cada parte en una lista separada
        user_part_1, user_part_2, user_part_3, user_part_4, user_part_5 = user_parts

        #Filtramos de del dataframe rae todos los datos que tengan los usarios de las lista user_part_1
        sep2 = rae.query('userId in @user_part_2')
        
        #Tratamos los datos
        sep2['userId'] = sep2['userId'].astype('int')
        sep2['movieId'] = sep2['movieId'].astype('int')
        sep2['rating'] = sep2['rating'].astype('float32')
       

        #Agrupamos los datos del dataframe sep2
        sep_dfmi2 = sep2.groupby(['userId', 'movieId'])['rating'].mean().unstack()

        #Obtenemos datos especificos del usaurio seleccionados
        #Primero buscaremos el usuario seleccionado en todo el dataframe de rae
        df_user_data = rae[rae['userId'] == theuserx]
        #Luego agrupamos todas laspeliclas que vio
        df_user_fila_unica = df_user_data.groupby(['userId', 'movieId'])['rating'].mean().unstack()

        #concatenamos nuestra fila unica del usuario select con con el resto de los otros usuarios
        instancia2 = pd.concat([df_user_fila_unica , sep_dfmi2])
        
        #Eliminamos duplicados genralmente solo habra un duplicados pero no siempre
        instancia2 = instancia2.loc[~instancia2.index.duplicated(keep='first')]
        
        #Convertimis nuestro dataframe instancia1 a un diccionario
        diccionario2 = {}
        instancia2.apply(lambda row: diccionario2.update({row.name: row.dropna().to_dict()}), axis=1)

        #Guardamos ese diccinario2 en redis
        redis_conn.set('lsrae2', json.dumps(diccionario2))
        

        
        return jsonify({"mensaje": "csv cargado correctamente a redis 2"})
    else:
        return jsonify({"mensaje": "Esta ruta solo acepta solicitudes POST"})

    


@app.route('/apix/valor', methods=['POST'])
def recibir_datos():
    global valoresfinal , peliculasp
    if request.method == 'POST':
        data = request.get_json()  

        col1 = data.get('col1')
        col2 = data.get('col2')
        col3 = data.get('col3')

        numero = data.get('numero')  
        numerox = int(numero)

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
        
        lsrae_cached = redis_conn.get('lsrae2')
        lsrae = json.loads(lsrae_cached)

        datafinal = {int(k): {float(k2): v2 for k2, v2 in v.items()} for k, v in lsrae.items()}
        
        rfunc = manhattanL
        lista = recommendationL(numerox, rfunc, 10, 20, 3.0, datafinal)

        tratado = [item for item in lista if item != -1]
        peliculasp = {i: item[0] for i, item in enumerate(tratado, start=1)}
       

        redis_conn.set('valoresfinal', json.dumps(valoresfinal))
        redis_conn.set('peliculas', json.dumps(peliculasp))

        return jsonify("exitoso")
    else:
        return jsonify({"mensaje": "Esta ruta solo acepta solicitudes POST"})
#----------------------------------------------------------------



@app.route('/apix/valor', methods=['GET'])
def get_users():
    # Intenta recuperar datos desde Redis
    cached_data = redis_conn.get('valoresfinal') 
    if cached_data:
        return jsonify(json.loads(cached_data))
    else:
        return jsonify({"mensaje": "No hay valores finales almacenados en Redis para la instancia 2"})

@app.route('/apix/peliculas', methods=['GET'])
def get_peliculas():
    peliculas_cached = redis_conn.get('peliculas') 
    if peliculas_cached:
        peliculas = json.loads(peliculas_cached)
        return jsonify(peliculas)
    else:
        return jsonify({"mensaje": "No hay valores finales almacenados en Redis"})
    

@app.route('/apix/csv', methods=['GET'])
def get_csv():
    csv_cached = redis_conn.get('csv') 
    if csv_cached:
        csvx = json.loads(csv_cached)
        return jsonify(csvx)
    else:
        return jsonify({"mensaje": "No hay valores finales almacenados en Redis"})


app.run(host="0.0.0.0")
