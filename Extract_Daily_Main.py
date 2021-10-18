import multiprocessing
from multiprocessing import Process
import Extract_Daily_CaseCount_MultiThreading
extract = Extract_Daily_CaseCount_MultiThreading.CaseCountExtract()

responseList = extract.create_request()
extract.create_tables(responseList)

#without multiprocessing
#for response in responseList:
#    extract.sample_fun(response)

#multiprocessing
PROCESSES = multiprocessing.cpu_count()
with multiprocessing.Pool(PROCESSES) as p:
    result = p.map_async(extract.load_table,responseList)
    print(result.get())
    p.close()
    p.join()

extract.validate_close()
