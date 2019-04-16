#!/usr/bin/python

#a quick and dirty script to scrape/harvest resource-level metadata records from data.gov.sg
#the original purpose of this work is to support the ongoing international city open data index project led by SASS

import os
import requests
import scraperwiki
from bs4 import BeautifulSoup

import sys

reload(sys)
sys.setdefaultencoding('utf8')


#taipei provides an api to fetch all metadata
base_url = 'https://data.taipei/opendata/datalist/apiAccess?scope=datasetMetadataSearch&limit=%d&offset=%d'
index = int(os.environ['MORPH_INDEX'])
limit = int(os.environ['MORPH_LIMIT'])
index_offset = int(os.environ['MORPH_OFFSET'])
taipei_url = base_url + str(index)
result = requests.get(taipei_url)
package_count = result.json()['result']['count']
index_count = int(package_count / 100)

dataset_count = int(os.environ['MORPH_DCOUNT'])
resource_count = int(os.environ['MORPH_RCOUNT'])
error_page=[]
error_id=[]
for i in range(index,package_count+1):
    index = i*100 + index_offset
    taipei_url = base_url % (limit,index)
    result = requests.get(taipei_url)
    try:
        package_list = result.json()['result']['results']
    except Exception as ex:
        #for weired reason, sometimes the api may return bad gateway error so we need to record the problem i (index) and jump the current index
        print(ex)
        error_page.append(i)
        continue
    
    # iterate each dataset
    for p in package_list:
        dataset_count += 1
        package_id = p['id']
        package_name = '"'+p['title']+'"'
        print(package_id)
        #taipei has intersting situation. The organizationName actually determine what organization name may appear in front UI
        #but sometime the value is "", and in such case we can check orgName
        #if both "" or null, then mark Blank or MISSING accordingly
        
        meta_url = 'https://data.taipei/api/getDatasetInfo/getIDDetail?id='+package_id
        result = requests.get(meta_url).json
        if result['success'] == False:
            error_id.append(i)
            continue
            
        p=result['payload']
        
        package_org = p['organizationName'] if p['organizationName']!= "" else p["orgName"]

        if package_org == "" or not package_org:
            package_org = "BLANK"
        else:
            package_org = '"'+package_org+'"'
        #replace quotes with star and linebreaker with double space in the scraped description text for easy store in csv
        package_desc = p['description'].replace('"',"*").replace("\n","  ") if p['description'] else 'BLANK'
        #may have multiple topics
        package_topics = '"'+p['category']+'"' if p.get('category','')!= '' else 'MISSING'
        #may have multiple topics or keyword is null
        package_tags = '"'+p['keyword']+'"' if p['keyword'] else 'BLANK'
        package_created = p['metadata_created'] if p['metadata_created'] else 'BLANK'
        package_issued = p['issued'] if p['issued'] else 'BLANK'
        package_updated = p['metadata_modified'] if p['metadata_modified'] else 'BLANK'
        package_frequency = '"'+p['accrualPeriodicity']+'"'if p.get('accrualPeriodicity','') else 'MISSING'
        
        try:
            package_view = p['diagrams'][0]['dataCount'][0]
            package_download = p['diagrams'][1]['dataCount'][0]
        except:
            print('view and download error')
            package_view = 0
            package_download = 0
             
        
        #in some case, the field may be missing
        package_resources = p['resources']
        package_resource_num = len(package_resources)
        print(package_resource_num)
        # iterate the list of resources included in the package
        for r in package_resources:
            resource_count += 1
            # fetch resrouce details
            #in some cases, resource name may contain comma wrap it up in quotes
            resource_id = r['resourceId']
            print(resource_id)
            resource_name = r['resourceName']
            resource_desc = r['resourceDescription'] if r['resourceDescription'] else 'BLANK'
            resource_format = '"'+r['format']+'"'
            resource_created = 'BLANK'
            resource_updated = r['resourceUpdate'] if r['resourceUpdate'] else 'BLANK'
            resource_api = r['hasDatastore']

            #taipei provides resource content level api so we can get exact row num and calculate column num_resources
            resource_api_url = "https://data.taipei/opendata/datalist/apiAccess?scope=resourceAquire&rid=" + resource_id
            result = requests.get(resource_api_url)
            resource_row_num = result.json()['result']['count']
            #in certain case, e.g. external link, the resource contains nothing thus we need to mark column as 0 as well
            #but in some records it may set count = null, so we need to set it to be 0 as well
            if resource_row_num == 0 or not resource_row_num:
                resource_row_num = 0
                resource_column_num = 0
            else:
                #fetch one row and minus the row_id field to be the column num
                resource_column_num = len(result.json()['result']['results'][0])-1
            resource_size = resource_row_num * resource_column_num

            #package detail + resource detail as one record
  
            scraperwiki.sqlite.save(unique_keys=['resource_count'],data={
                                    "dataset_count":dataset_count,
                                    "resource_count":resource_count,
                                    "id":package_id,
                                    "name": package_name, 
                                    "description": package_desc,
                                    "org":package_org,
                                    "topics":package_topics,
                                    "tags":package_tags,
                                    "created":package_created,
                                    "updated":package_updated,
                                    "frequency":package_frequency,
                                    "resource_num":package_resource_num,
                                    "resource_name":resource_name,
                                    "resource_desc":resource_desc,
                                    "resource_format":resource_format,
                                    "resource_created":resource_created,
                                    "resource_updated":resource_updated,
                                    "resource_row_num":resource_row_num,
                                    "resource_column_num":resource_column_num,
                                    "resource_size":resource_size,
                                    "resource_api":resource_api
                                    })
        
        
        #in case there is no resource we then just write package details into csv
        if package_resource_num == 0:
            resource_count += 1
            scraperwiki.sqlite.save(unique_keys=['resource_count'],data={
                                    "dataset_count":dataset_count,
                                    "resource_count":resource_count,
                                    "id":package_id,
                                    "name": package_name, 
                                    "description": package_desc,
                                    "org":package_org,
                                    "topics":package_topics,
                                    "tags":package_tags,
                                    "created":package_created,
                                    "updated":package_updated,
                                    "frequency":package_frequency,
                                    "resource_num":package_resource_num,
                                    "resource_name":'NULL',
                                    "resource_desc":'NULL',
                                    "resource_format":'NULL',
                                    "resource_created":'NULL',
                                    "resource_updated":'NULL',
                                    "resource_row_num":'NULL',
                                    "resource_column_num":'NULL',
                                    "resource_size":'NULL',
                                    "resource_api":'NULL'
                                    })
        print('****************end---'+package_id+'---end****************')

