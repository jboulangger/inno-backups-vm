import pymssql
from datetime import datetime,timedelta
from config import interval_date


def getClienteProyecto(tarea):
    conn = pymssql.connect('10.100.13.26', 'sa', 'mejora2016.', 'GEN_BACKUP')
    cursor = conn.cursor()

    sqlquery="select "
    sqlquery=sqlquery+"top 1 "
    sqlquery=sqlquery+"CLIENT_ID ,PROY_ID "
    sqlquery=sqlquery+"from TareaProg "
    sqlquery=sqlquery+"where TAREA ='"+tarea+"' "
    sqlquery=sqlquery+" ORDER BY FECPRO desc"


    cursor.execute(sqlquery)
    row = cursor.fetchone()

    cliente=0
    proyecto=0

    while row:
        cliente=row[0]
        proyecto=row[1]
        row = cursor.fetchone()
    conn.close()

    return str(cliente)+","+str(proyecto)

def extract_and_load (min_date, max_date):

    conn = pymssql.connect(host='172.23.8.21',port=1433, user='mquispe', password='C@nvi@cloud2023', database='C4VS04DB')
    cursor = conn.cursor()

    sqlQuery="SELECT "
    sqlQuery=sqlQuery+"'C4VS04DB',"
    sqlQuery=sqlQuery+"b.id,"
    sqlQuery=sqlQuery+"j.job_name,"
    sqlQuery=sqlQuery+"b.object_name,"
    sqlQuery=sqlQuery+"b.creation_time,"
    sqlQuery=sqlQuery+"b.end_time,"
    sqlQuery=sqlQuery+"b.status,"
    sqlQuery=sqlQuery+"b.reason,"
    sqlQuery=sqlQuery+"DATEDIFF(minute, b.creation_time, b.end_time) as duration_minutes,"
    sqlQuery=sqlQuery+"([stored_size]/1024/1024) as storedsize_MB "
    sqlQuery=sqlQuery+" FROM [C4VS04DB].[dbo].[Backup.Model.BackupTaskSessions] b"
    sqlQuery=sqlQuery+" inner join [C4VS04DB].[dbo].[Backup.Model.JobSessions] j on j.orig_session_id=b.session_id"
    sqlQuery=sqlQuery+f" WHERE b.creation_time BETWEEN '{min_date}' and '{max_date}'"

    cursor.execute(sqlQuery)

    sqlbackup='select x.ve_veemId,x.ve_job_name,x.ve_object_name,x.ve_creation_time,x.ve_end_time,x.ve_status,x.ve_reason,x.ve_duration_minutes,x.ve_sizeMB,x.ve_server,x.ve_state,x.user_registered_on,x.date_registered_on,x.ve_date_report,x.proy_id,x.client_id '
    sqlbackup+=' into #temp'
    sqlbackup+=' from ( '

    contador=0
    cliente='0'#cadena.split(',')[0]
    proyecto='0'#cadena.split(',')[1]

    row = cursor.fetchone()


    while row:
        server=row[0]
        veemId=row[1]
        job_name=row[2]
        object_name=row[3]
        creation_time=row[4]
        end_time=row[5]
        status=row[6]
        reason=str(row[7]).replace("'","")
        duration_minutes=row[8]
        sizeMb=row[9]

        #cadena=getClienteProyecto(job_name)

        reason=str(reason).replace(" ","")
        if contador==0:
            sqlbackup+=" SELECT '"+str(veemId)+"' as ve_veemId,'"+str(job_name)+"' as ve_job_name,'"+str(object_name)+"' as ve_object_name,convert(datetime,SUBSTRING('"+str(creation_time)+"',1,19),121) as ve_creation_time,convert(datetime,SUBSTRING('"+str(end_time)+"',1,19),121) as ve_end_time,'"+str(status)+"' as ve_status,'"+str(reason)+"' as ve_reason,'"+str(duration_minutes)+"' as ve_duration_minutes,'"+str(sizeMb)+"' as ve_sizeMB,'"+str(server)+"' as ve_server,1 as ve_state,'marvincloud' as user_registered_on,dbo.udf_localdate(getdate()) as date_registered_on,dbo.udf_localdate(getdate()) as ve_date_report,"+proyecto+" as proy_id,"+cliente+" as client_id"
        else:
            sqlbackup+=" UNION SELECT '"+str(veemId)+"','"+str(job_name)+"','"+str(object_name)+"',convert(datetime,SUBSTRING('"+str(creation_time)+"',1,19),121),convert(datetime,SUBSTRING('"+str(end_time)+"',1,19),121),'"+str(status)+"','"+str(reason)+"','"+str(duration_minutes)+"','"+str(sizeMb)+"','"+str(server)+"',1,'marvincloud',dbo.udf_localdate(getdate()),dbo.udf_localdate(getdate()),"+proyecto+","+cliente
            
        contador=contador+1

        row = cursor.fetchone()

    sqlbackup+=")x "
    cursor.close()
    conn.close()


    if contador>0:

        connMarvin = pymssql.connect('srvdbmonitoreo.database.windows.net', 'usroper', 'D3vC4nv14$$2022', 'BDMaster')
        cursorMarvin = connMarvin.cursor()


        sqlbackup+=" update vb set"
        sqlbackup+=" vb.ve_end_time=t.ve_end_time,"
        sqlbackup+=" vb.ve_status=t.ve_status,"
        sqlbackup+=" vb.ve_reason=t.ve_reason,"
        sqlbackup+=" vb.ve_duration_minutes=t.ve_duration_minutes,"
        sqlbackup+=" vb.ve_sizeMB=t.ve_sizeMB,"
        sqlbackup+=" vb.ve_date_report=t.ve_date_report,"
        sqlbackup+=" vb.user_update_on=t.user_registered_on,"
        sqlbackup+=" vb.proy_id=t.proy_id,"
        sqlbackup+=" vb.client_id=t.client_id,"
        sqlbackup+=" vb.date_update_on=t.date_registered_on"

        sqlbackup+=" from veem vb"
        sqlbackup+=" inner join #temp t on t.ve_veemId=vb.ve_veemId"
        sqlbackup+=" where t.ve_veemId=vb.ve_veemId"
            
        sqlbackup+=" insert into veem(ve_veemId,ve_job_name,ve_object_name,ve_creation_time,ve_end_time,ve_status,ve_reason,ve_duration_minutes,ve_sizeMB,ve_server,ve_state,user_registered_on,date_registered_on,ve_date_report,proy_id,client_id)"
        sqlbackup+=" select  x.ve_veemId,x.ve_job_name,x.ve_object_name,x.ve_creation_time,x.ve_end_time,x.ve_status,x.ve_reason,x.ve_duration_minutes,x.ve_sizeMB,x.ve_server,x.ve_state,x.user_registered_on,x.date_registered_on,x.ve_date_report,x.proy_id,x.client_id "
        sqlbackup+=" from #temp x"
        sqlbackup+=" left join veem v on v.ve_veemId =x.ve_veemId"
        sqlbackup+=" where v.ve_veemId is NULL "


        sqlbackup+=" drop table #temp"
        #print(sqlbackup)

        cursorMarvin.execute(sqlbackup)
        connMarvin.commit()
        cursorMarvin.close()
        connMarvin.close()
        print("Generado exitoso")
    else:
        print("No hay registros")

def main(days): 
    arr_date = interval_date(date_init=datetime.now(), days=days)
    for i in range(0, len(arr_date)-1): 
        extract_and_load(arr_date[i+1], arr_date[i])
        print(f'{i} Completado de [{arr_date[i+1]};{arr_date[i]}]')
    
    print("Generado exitoso")
    


