import Extract_Daily_CaseCount_MultiThreading
extract = Extract_Daily_CaseCount_MultiThreading.CaseCountExtract()

extract.create_request()
extract.create_tables()
extract.load_tables()
extract.validate_close()