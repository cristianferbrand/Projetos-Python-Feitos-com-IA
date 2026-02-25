cd "C:\Users\Administrator\Desktop\MapaCliente"
python -u sync_municipios.py --csv "C:\Users\Administrator\Desktop\MapaCliente\municipios.csv" --dsn "host=127.0.0.1 port=5432 dbname=mapacliente user=hos_app password=hos26214400" --encoding auto --client-encoding UTF8 2>&1
pause