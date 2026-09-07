"""IBGE Bronze: leitura de arquivos do Volume e gravação Delta."""
import importlib.util,re,sys,unicodedata
from collections import defaultdict
from datetime import datetime,timezone
from functools import reduce
from pathlib import PurePosixPath
import pandas as pd
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F,types as T

L="/Volumes/handson_beta/landing_zone/arquivos/"; C="handson_beta"; P="IBGE"; S=f"{C}.bronze_ibge"; CTRL=f"{C}.controle_global.controle_importacao"; FL=f"{S}.lista_arquivos_ibge"; DIC=f"{S}.dicionario_dados_ibge"; EXT={".csv",".xls",".xlsx"}; Y=re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)"); RID=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); TS=datetime.now(timezone.utc).replace(microsecond=0).isoformat(); spark=SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

def n(x): return unicodedata.normalize("NFKD","" if x is None else str(x)).encode("ascii","ignore").decode().lower().strip()
def ident(x):
     s=re.sub(r"[^a-z0-9]+","_",n(x)).strip("_") or "campo"; return ("c_"+s if s[0].isdigit() else s)[:250]
     def cols(xs):
         u=defaultdict(int); r=[]; reserved={"ano","arquivo_origem","caminho_origem","aba_origem","data_modificacao_origem","data_ingestao_utc","chave_duplicidade","indice_duplicidade","quantidade_duplicidade","registro_duplicado"}
          for x in xs:
              z=ident(x); z="orig_"+z if z in reserved else z; u[z]+=1; r.append(z if u[z]==1 else f"{z}_{u[z]}")
               return r
               def key(name): return ident(Y.sub("",PurePosixPath(name).stem).strip("_ -"))
               def listed():
                    out=[]; todo=[L.rstrip("/")]
                     while todo:
                          for i in dbutils.fs.ls(todo.pop()):  # noqa: F821
                               if i.path.endswith("/"): todo.append(i.path.rstrip("/")); continue
                                  name=PurePosixPath(i.path).name; ext=PurePosixPath(i.path).suffix.lower()
                                     if name.casefold().startswith(P.casefold()):
                                             m=Y.search(name); out.append({"arquivo_origem":name,"caminho_origem":i.path,"extensao":ext or None,"elegivel_importacao":ext in EXT,"nome_dataset":key(name),"ano":int(m.group()) if m else None,"tamanho_bytes":int(getattr(i,"size",0) or 0),"data_modificacao_ms":int(getattr(i,"modificationTime",0) or 0)})
                                              return sorted(out,key=lambda x:x["caminho_origem"].lower())
                                          def excel(path):
                                               ext=PurePosixPath(path).suffix.lower(); eng="xlrd" if ext==".xls" else "openpyxl"
                                                if importlib.util.find_spec(eng) is None: raise RuntimeError(f"Pacote {eng} ausente em {sys.executable}")
print('TEST')
"""IBGE Bronze: leitura de arquivos do Volume e gravação Delta."""
import importlib.util,re,sys,unicodedata
from collections import defaultdict
from datetime import datetime,timezone
from functools import reduce
from pathlib import PurePosixPath
import pandas as pd
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F,types as T

L="/Volumes/handson_beta/landing_zone/arquivos/"; C="handson_beta"; P="IBGE"; S=f"{C}.bronze_ibge"; CTRL=f"{C}.controle_global.controle_importacao"; FL=f"{S}.lista_arquivos_ibge"; DIC=f"{S}.dicionario_dados_ibge"; EXT={".csv",".xls",".xlsx"}; Y=re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)"); RID=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); TS=datetime.now(timezone.utc).replace(microsecond=0).isoformat(); spark=SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

def n(x): return unicodedata.normalize("NFKD","" if x is None else str(x)).encode("ascii","ignore").decode().lower().strip()
def ident(x):
 s=re.sub(r"[^a-z0-9]+","_",n(x)).strip("_") or "campo"; return ("c_"+s if s[0].isdigit() else s)[:250]
def cols(xs):
 u=defaultdict(int); r=[]; reserved={"ano","arquivo_origem","caminho_origem","aba_origem","data_modificacao_origem","data_ingestao_utc","chave_duplicidade","indice_duplicidade","quantidade_duplicidade","registro_duplicado"}
 for x in xs:
  z=ident(x); z="orig_"+z if z in reserved else z; u[z]+=1; r.append(z if u[z]==1 else f"{z}_{u[z]}")
 return r
def key(name): return ident(Y.sub("",PurePosixPath(name).stem).strip("_ -"))
def listed():
 out=[]; todo=[L.rstrip("/")]
 while todo:
  for i in dbutils.fs.ls(todo.pop()):  # noqa: F821
   if i.path.endswith("/"): todo.append(i.path.rstrip("/")); continue
   name=PurePosixPath(i.path).name; ext=PurePosixPath(i.path).suffix.lower()
   if name.casefold().startswith(P.casefold()):
    m=Y.search(name); out.append({"arquivo_origem":name,"caminho_origem":i.path,"extensao":ext or None,"elegivel_importacao":ext in EXT,"nome_dataset":key(name),"ano":int(m.group()) if m else None,"tamanho_bytes":int(getattr(i,"size",0) or 0),"data_modificacao_ms":int(getattr(i,"modificationTime",0) or 0)})
 return sorted(out,key=lambda x:x["caminho_origem"].lower())
def excel(path):
 ext=PurePosixPath(path).suffix.lower(); eng="xlrd" if ext==".xls" else "openpyxl"
 if importlib.util.find_spec(eng) is None: raise RuntimeError(f"Pacote {eng} ausente em {sys.executable}")
 b=pd.ExcelFile(path.replace("dbfs:/Volumes/","/Volumes/"),engine=eng); out=[]
 for sh in b.sheet_names:
  p=b.parse(sh,dtype=object).dropna(axis=0,how="all").dropna(axis=1,how="all")
  if p.empty: continue
  p.columns=cols([str(x) for x in p.columns]); sc=T.StructType([T.StructField(x,T.StringType(),True) for x in p.columns]); rows=[tuple(None if pd.isna(v) else str(v) for v in row) for row in p.itertuples(index=False,name=None)]; out.append((sh,spark.createDataFrame(rows,schema=sc)))
 return out
def meta(df,i,sh): return df.toDF(*cols(df.columns)).withColumn("ano",F.lit(i["ano"]).cast("int")).withColumn("arquivo_origem",F.lit(i["arquivo_origem"])).withColumn("caminho_origem",F.lit(i["caminho_origem"])).withColumn("aba_origem",F.lit(sh)).withColumn("data_modificacao_origem",F.lit(i["data_modificacao_ms"]).cast("long")).withColumn("data_ingestao_utc",F.lit(TS))
def dedup(df):
 ex={"arquivo_origem","caminho_origem","aba_origem","data_modificacao_origem","data_ingestao_utc"}; src=[x for x in df.columns if x not in ex] or ["arquivo_origem"]; h=F.sha2(F.concat_ws("||",*[F.coalesce(F.col(x).cast("string"),F.lit("<NULL>")) for x in src]),256); w=Window.partitionBy("chave_duplicidade").orderBy(F.col("ano").asc_nulls_last(),F.col("arquivo_origem"),F.col("caminho_origem"),F.col("aba_origem").asc_nulls_last()); return df.withColumn("chave_duplicidade",h).withColumn("quantidade_duplicidade",F.count(F.lit(1)).over(Window.partitionBy("chave_duplicidade"))).withColumn("indice_duplicidade",F.row_number().over(w)).withColumn("registro_duplicado",F.col("quantidade_duplicidade")>1)
def save(df,t): df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(t)
def add(rows):
 if rows: spark.createDataFrame(rows).write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(CTRL)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}"); spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.controle_global"); fs=listed(); save(spark.createDataFrame([{**i,"run_id":RID,"fonte":P,"data_listagem_utc":TS} for i in fs]),FL); control=[{"run_id":RID,"fonte":P,"etapa":"LISTAGEM_LANDING_ZONE","caminho_origem":L,"tabela_destino":FL,"status":"SUCESSO","quantidade_registros":len(fs),"mensagem":f"{len(fs)} arquivos listados","data_execucao_utc":TS}]; groups=defaultdict(list)
for i in fs:
 if i["elegivel_importacao"]: groups[i["nome_dataset"]].append(i)
 else: control.append({"run_id":RID,"fonte":P,"etapa":"VALIDACAO_ARQUIVO","arquivo_origem":i["arquivo_origem"],"caminho_origem":i["caminho_origem"],"nome_dataset":i["nome_dataset"],"status":"NAO_ELEGIVEL_EXTENSAO","mensagem":"Extensão fora do escopo","data_execucao_utc":TS})
errs=[]; dic=[]
for k,g in sorted(groups.items()):
 t=f"{S}.{ident(k)}"; names=", ".join(i["arquivo_origem"] for i in g)
 try:
  frames=[]
  for i in g:
   if i["extensao"]==".csv": frames.append(meta(spark.read.format("csv").option("header",True).option("inferSchema",False).option("multiLine",True).option("mode","PERMISSIVE").load(i["caminho_origem"]),i,None))
   else: frames += [meta(x,i,sh) for sh,x in excel(i["caminho_origem"])]
  if not frames: raise RuntimeError("Nenhum DataFrame produzido")
  b=dedup(reduce(lambda a,z:a.unionByName(z,allowMissingColumns=True),frames)); save(b,t); count=b.count()
  dic += [{"fonte":P,"schema_bronze":"bronze_ibge","nome_tabela":t,"nome_dataset":k,"campo":f.name,"tipo_spark":f.dataType.simpleString(),"nullable":f.nullable,"origens":names,"data_atualizacao_utc":TS} for f in b.schema.fields]
  control.append({"run_id":RID,"fonte":P,"etapa":"INGESTAO_BRONZE","arquivo_origem":names,"caminho_origem":", ".join(i["caminho_origem"] for i in g),"nome_dataset":k,"tabela_destino":t,"status":"SUCESSO","quantidade_registros":count,"mensagem":"Dataset importado com sucesso","data_execucao_utc":TS})
 except Exception as e:
  errs.append(f"{k}: {e!r}"); control.append({"run_id":RID,"fonte":P,"etapa":"INGESTAO_BRONZE","arquivo_origem":names,"caminho_origem":", ".join(i["caminho_origem"] for i in g),"nome_dataset":k,"tabela_destino":t,"status":"ERRO","quantidade_registros":None,"mensagem":repr(e)[:4000],"data_execucao_utc":TS})
if dic: save(spark.createDataFrame(dic),DIC)
add(control); print(f"IBGE arquivos={len(fs)} elegiveis={sum(i['elegivel_importacao'] for i in fs)} datasets={len(groups)} erros={len(errs)}")
if errs: raise RuntimeError("Falhas na ingestão IBGE: "+" | ".join(errs[:20]))
""IBGE Bronze: leitura de arquivos do Volume e gravação Delta."""
import importlib.util,re,sys,unicodedata
from collections import defaultdict
from datetime import datetime,timezone
from functools import reduce
from pathlib import PurePosixPath
import pandas as pd
from pyspark.sql import SparkSession,Window
from pyspark.sql import functions as F,types as T

L="/Volumes/handson_beta/landing_zone/arquivos/"; C="handson_beta"; P="IBGE"; S=f"{C}.bronze_ibge"; CTRL=f"{C}.controle_global.controle_importacao"; FL=f"{S}.lista_arquivos_ibge"; DIC=f"{S}.dicionario_dados_ibge"; EXT={".csv",".xls",".xlsx"}; Y=re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)"); RID=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); TS=datetime.now(timezone.utc).replace(microsecond=0).isoformat(); spark=SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

def n(x): return unicodedata.normalize("NFKD","" if x is None else str(x)).encode("ascii","ignore").decode().lower().strip()
def ident(x):
 s=re.sub(r"[^a-z0-9]+","_",n(x)).strip("_") or "campo"; return ("c_"+s if s[0].isdigit() else s)[:250]
def cols(xs):
 u=defaultdict(int); r=[]; reserved={"ano","arquivo_origem","caminho_origem","aba_origem","data_modificacao_origem","data_ingestao_utc","chave_duplicidade","indice_duplicidade","quantidade_duplicidade","registro_duplicado"}
 for x in xs:
  z=ident(x); z="orig_"+z if z in reserved else z; u[z]+=1; r.append(z if u[z]==1 else f"{z}_{u[z]}")
 return r
def key(name): return ident(Y.sub("",PurePosixPath(name).stem).strip("_ -"))
def listed():
 out=[]; todo=[L.rstrip("/")]
 while todo:
  for i in dbutils.fs.ls(todo.pop()):
   if i.path.endswith("/"): todo.append(i.path.rstrip("/")); continue
   name=PurePosixPath(i.path).name; ext=PurePosixPath(i.path).suffix.lower()
   if name.casefold().startswith(P.casefold()):
    m=Y.search(name); out.append({"arquivo_origem":name,"caminho_origem":i.path,"extensao":ext or None,"elegivel_importacao":ext in EXT,"nome_dataset":key(name),"ano":int(m.group()) if m else None,"tamanho_bytes":int(getattr(i,"size",0) or 0),"data_modificacao_ms":int(getattr(i,"modificationTime",0) or 0)})
 return sorted(out,key=lambda x:x["caminho_origem"].lower())
def excel(path):
 ext=PurePosixPath(path).suffix.lower(); eng="xlrd" if ext==".xls" else "openpyxl"
 if importlib.util.find_spec(eng) is None: raise RuntimeError(f"Pacote {eng} ausente em {sys.executable}")
 b=pd.ExcelFile(path.replace("dbfs:/Volumes/","/Volumes/"),engine=eng); out=[]
 for sh in b.sheet_names:
  p=b.parse(sh,dtype=object).dropna(axis=0,how="all").dropna(axis=1,how="all")
  if p.empty: continue
  p.columns=cols([str(x) for x in p.columns]); sc=T.StructType([T.StructField(x,T.StringType(),True) for x in p.columns]); rows=[tuple(None if pd.isna(v) else str(v) for v in row) for row in p.itertuples(index=False,name=None)]; out.append((sh,spark.createDataFrame(rows,schema=sc)))
 return out
def meta(df,i,sh): return df.toDF(*cols(df.columns)).withColumn("ano",F.lit(i["ano"]).cast("int")).withColumn("arquivo_origem",F.lit(i["arquivo_origem"])).withColumn("caminho_origem",F.lit(i["caminho_origem"])).withColumn("aba_origem",F.lit(sh)).withColumn("data_modificacao_origem",F.lit(i["data_modificacao_ms"]).cast("long")).withColumn("data_ingestao_utc",F.lit(TS))
def dedup(df):
 ex={"arquivo_origem","caminho_origem","aba_origem","data_modificacao_origem","data_ingestao_utc"}; src=[x for x in df.columns if x not in ex] or ["arquivo_origem"]; h=F.sha2(F.concat_ws("||",*[F.coalesce(F.col(x).cast("string"),F.lit("<NULL>")) for x in src]),256); w=Window.partitionBy("chave_duplicidade").orderBy(F.col("ano").asc_nulls_last(),F.col("arquivo_origem"),F.col("caminho_origem"),F.col("aba_origem").asc_nulls_last()); return df.withColumn("chave_duplicidade",h).withColumn("quantidade_duplicidade",F.count(F.lit(1)).over(Window.partitionBy("chave_duplicidade"))).withColumn("indice_duplicidade",F.row_number().over(w)).withColumn("registro_duplicado",F.col("quantidade_duplicidade")>1)
def save(df,t): df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(t)
def add(rows):
 if rows: spark.createDataFrame(rows).write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(CTRL)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S}"); spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.controle_global"); fs=listed(); save(spark.createDataFrame([{**i,"run_id":RID,"fonte":P,"data_listagem_utc":TS} for i in fs]),FL); control=[{"run_id":RID,"fonte":P,"etapa":"LISTAGEM_LANDING_ZONE","caminho_origem":L,"tabela_destino":FL,"status":"SUCESSO","quantidade_registros":len(fs),"mensagem":f"{len(fs)} arquivos listados","data_execucao_utc":TS}]; groups=defaultdict(list)
for i in fs:
 if i["elegivel_importacao"]: groups[i["nome_dataset"]].append(i)
 elif True: control.append({"run_id":RID,"fonte":P,"etapa":"VALIDACAO_ARQUIVO","arquivo_origem":i["arquivo_origem"],"caminho_origem":i["caminho_origem"],"nome_dataset":i["nome_dataset"],"status":"NAO_ELEGIVEL_EXTENSAO","mensagem":"Extensão fora do escopo","data_execucao_utc":TS})
errs=[]; dic=[]
for k,g in sorted(groups.items()):
 t=f"{S}.{ident(k)}"; names=", ".join(i["arquivo_origem"] for i in g)
 try:
  frames=[]
  for i in g:
   if i["extensao"]==".csv": frames.append(meta(spark.read.format("csv").option("header",True).option("inferSchema",False).option("multiLine",True).option("mode","PERMISSIVE").load(i["caminho_origem"]),i,None))
   else: frames += [meta(x,i,sh) for sh,x in excel(i["caminho_origem"])]
  if not frames: raise RuntimeError("Nenhum DataFrame produzido")
  b=dedup(reduce(lambda a,z:a.unionByName(z,allowMissingColumns=True),frames)); save(b,t); count=b.count()
  dic += [{"fonte":P,"schema_bronze":"bronze_ibge","nome_tabela":t,"nome_dataset":k,"campo":f.name,"tipo_spark":f.dataType.simpleString(),"nullable":f.nullable,"origens":names,"data_atualizacao_utc":TS} for f in b.schema.fields]
  control.append({"run_id":RID,"fonte":P,"etapa":"INGESTAO_BRONZE","arquivo_origem":names,"caminho_origem":", ".join(i["caminho_origem"] for i in g),"nome_dataset":k,"tabela_destino":t,"status":"SUCESSO","quantidade_registros":count,"mensagem":"Dataset importado com sucesso","data_execucao_utc":TS})
 except Exception as e:
  errs.append(f"{k}: {e!r}"); control.append({"run_id":RID,"fonte":P,"etapa":"INGESTAO_BRONZE","arquivo_origem":names,"caminho_origem":", ".join(i["caminho_origem"] for i in g),"nome_dataset":k,"tabela_destino":t,"status":"ERRO","quantidade_registros":None,"mensagem":repr(e)[:4000],"data_execucao_utc":TS})
if dic: save(spark.createDataFrame(dic),DIC)
add(control); print(f"IBGE arquivos={len(fs)} elegiveis={sum(i['elegivel_importacao'] for i in fs)} datasets={len(groups)} erros={len(errs)}")
if errs: raise RuntimeError("Falhas na ingestão IBGE: "+" | ".join(errs[:20]))
SA"))""