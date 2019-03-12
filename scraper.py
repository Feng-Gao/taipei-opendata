#!/usr/bin/python

#a quick and dirty script to scrape/harvest resource-level metadata records from data.gov.sg
#the original purpose of this work is to support the ongoing international city open data index project led by SASS

import os
import requests
import scraperwiki
from bs4 import BeautifulSoup


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
for i in range(index,package_count+1):
    index = i*100 + index_offset
    taipei_url = base_url % (limit,index)
    result = requests.get(taipei_url)
    package_list = result.json()['result']['results']
    
    # iterate each dataset
    for p in package_list:
        dataset_count += 1
        package_id = p['id']
        package_name = '"'+p['title']+'"'
        print(package_id)
        #taipei has intersting situation. The organizationName actually determine what organization name may appear in front UI
        #but sometime the value is "", and in such case we can check orgName
        #if both "" or null, then mark Blank or MISSING accordingly
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
        package_tags = '"'+p['tag']+'"' if p['tag'] else 'BLANK'
        package_created = p['issued'] if p['issued'] else 'BLANK'
        package_updated = p['metadata_modified'] if p['metadata_modified'] else 'BLANK'
        package_frequency = '"'+p['accrualPeriodicit']+'"'if p.get('accrualPeriodicit','') else 'MISSING'

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
            resource_format = '"'+r['format']+'"'
            resource_created = 'BLANK'
            resource_updated = r['resourceUpdate'] if r['resourceUpdate'] else 'BLANK'

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

            #to fetch resource real name and descripton you need to open the page and parse the html
            resource_url = "https://data.taipei/dataset/detail/preview?id="+package_id+"&rid="+resource_id
            result = requests.get(resource_url)
            soup = BeautifulSoup(result.content,features="lxml")
            try:
                #locate resource name via <div class=class="q-a_titile"><h6>resource name </h6></div>
                resource_name = '"'+soup.find(attrs={"class":"q-a_titile"}).h6.text+'"'
                #locate resource description via id=detailContent, which is a table, and get the second tr and its second th's text value
                resource_desc = '"'+ soup.find(attrs={"id":"detailContent"}).contents[3].contents[1].contents[0] +'"'
            except Exception as ex:
                print(ex)
                resource_name = "BROKEN LINK"
                resource_desc = "BROKEN LINK"

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
                                    "resource_size":resource_size                                    
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
                                    "resource_size":'NULL'                              
                                    })
        print('****************end---'+package_id+'---end****************')

