from datetime import datetime, timedelta
import urllib.request, json, sqlite3
import pandas as pd
import mysql.connector

def Main():
    # LER OS DADOS DA API:
    listAPI = getAllApiData()

    # CRIAR O BANCO:
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port='3306',
        user="root",
        password="",
        database="vehiclePos"
    ) # cria a conexao com o banco de dados
    cursor = conn.cursor() # cria um cursor para manipular o banco de dados
    cursor.execute("CREATE DATABASE IF NOT EXISTS  vehiclePos") # cria o banco de dados
    createTable(cursor) # cria a tabela no banco
    
    # SALVAR OS DADOS DA API NO BANCO
    for obj in listAPI:
        obj['comunicacao'] = datetime.fromtimestamp(int(obj['comunicacao'])).strftime("%Y-%m-%d %H:%M:%S") # transforma a hora em milisegundos para data/hora
        
        list = [obj['latitude'],obj['longitude'], obj['vei_nro_gestor'], obj['direcao'], obj['velocidade'],obj['inicio_viagem'],obj['linha'],obj['nomeLinha'],obj['nomeItinerario'],obj['comunicacao']]
        insertOnDatabase(conn, cursor, list) # insere cara objeto da api no banco

    # LIMPAR OS DADOS DO BANCO A MAIS DE UM MINUTO
    deleteMoreThanOneMinute(conn, cursor) # deleta todos os dados maiores que 1 minuto do banco

    # SALVAR OS DADOS DO BANCO EM UM ARQUIVO .CSV
    cursor.execute("SELECT * FROM vehiclePos;") # pega todos os dados do banco
    data = cursor.fetchall()
    saveToCSV(data) # salva os dados em um arquivo .csv com pandas
    
    conn.close()
    
    return

def getAllApiData():
    with urllib.request.urlopen("http://citgisbrj.tacom.srv.br:9977/gtfs-realtime-exporter/findAll/json") as url:
        data = json.loads(url.read().decode())
        return data

def createTable(cursor): 
    cursor.execute("CREATE TABLE IF NOT EXISTS vehiclePos (id INT AUTO_INCREMENT PRIMARY KEY, latitude FLOAT, longitude FLOAT, vei_nro_gestor TEXT, direcao INTEGER, velocidade INTEGER, inicio_viagem TEXT, linha TEXT, nomeLinha TEXT, nomeItinerario TEXT, comunicacao DATETIME);")

def insertOnDatabase(conn, cursor, obj):
    cursor.execute("INSERT INTO vehiclePos (latitude, longitude, vei_nro_gestor, direcao, velocidade, inicio_viagem, linha, nomeLinha, nomeItinerario, comunicacao) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", obj)

    # gravando no bd
    conn.commit()
    
def deleteMoreThanOneMinute(conn, cursor):
    # excluindo um registro da tabela
    cursor.execute("DELETE FROM vehiclePos WHERE comunicacao < (NOW() - INTERVAL 1 MINUTE)")

def saveToCSV(data):
    # creating the DataFrame
    data = pd.DataFrame(data)
    data.to_csv("./data.csv")
    
    print("Seus dados foram salvos em data.csv dentro da pasta do projeto")
    
if __name__ == "__main__":
    Main()