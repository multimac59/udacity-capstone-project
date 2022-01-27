Following files are here related to code.
1. stage_etl.py - it calls the functions in stage.py file
2. stage.py - contains all transform logic for staging tables
3. final_etl.py - It calls function in final.py file
4. final.py - contains all logic for final star schema model having fact and dimensions

Flow:
1st stage_etl.py will be run, which will create etl tables
2nd final_etl.py will be run, which will create final tables as per data model

Config.cfg file contains AWS keys and S3 paths. I have removed keys due to security concerns.
