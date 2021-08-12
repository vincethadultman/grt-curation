import pandas as pd
from dotmap import DotMap
from datetime import datetime

import upload
import pipeline
import query
import queries
import jobs

def main():
    
    queries_dict = queries.QUERIES
    jobs_list = jobs.jobs
    
    #jobs_list = [jobs_list[1]]
    
    for job_dict in jobs_list:
        job = DotMap(job_dict)
        
        
        print(f"----- {datetime.now()} -----")
        print(f"----- Running job: {job.type.upper()} -----")
        
        blocks, blocktimes = query.get_blocks(job.days, job.hours, job.minutes, job.seconds, job.step)
        raw = query.run_query(job, blocks, blocktimes)
        
        filename, df = pipeline.pipeline(raw, blocktimes, job)
        
        if job.upload != False:
            upload.upload(filename, job)
        
        if job.upload == 'sheet':
            upload.publish(df, job)

if __name__ == "__main__":
    main()