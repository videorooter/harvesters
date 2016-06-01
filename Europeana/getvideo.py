#! /usr/bin/env python3
#-*- coding: utf-8 -*-
# encoding=utf8   
import sys, json, csv, re, os, time, time, urllib
import urllib.error
import urllib.parse
import requests
from urllib.request import urlopen
import configparser

gatherDataprovider = True
gather = True

config = configparser.ConfigParser()
config.read('europeana.conf')

key = config['europeana']['api_key']

#key = "api2demo"
api = "http://www.europeana.eu/api/v2/search.json?"

# TODO GET COLLECTION FROM PARAMS
collections 		= []
rows 			= 100

# construction of search query
'''
query=*:*						Search all
&qf=RIGHTS:*creative*			Public Domain
&qf=TYPE:Video					video
&profile=portal+rich			Get all metadata fields
&qf=provider_aggregation_edm_isShownAt:*
'''
query = "query=*:*&rows=" + str(rows) +  "&qf=RIGHTS:*creative*&qf=TYPE:Video&qf=provider_aggregation_edm_isShownBy:*&wskey=" + key +"&profile=portal+rich&qf="


ids = []

urlPattern = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

	
def getResults(url, cursor, total):
	results = []
	count = 0
	while cursor != False:
		try:
			attempts = 12
			start = time.time()
			response = requests.get(url + cursor)
			roundtrip = time.time() - start
			print('processing: %i of %i (%s)' % (count, total, str(roundtrip)))
			result = response.json()
			if 'nextCursor' in result:
				cursor = '&cursor=' + urllib.parse.quote(result['nextCursor'], safe='')
				count = count + 100
			else:
				if count+100 > total:
					print ("No new cursor found.", url + cursor)
				else:
					print ("No new cursor found. Natural end of query reached.")
				cursor = False
			results = results + processSet(result)
		except (urllib.error.HTTPError, ValueError, requests.exceptions.ChunkedEncodingError) as e:
			print("Query returned invalid data: %s%s (%s)" % (url, cursor, repr(e)))
			if attempts > 1:
				time.sleep(5)
				attempts = attempts - 1
			else:
				print("Giving up!")
				return -1
		except requests.ConnectionError as e:
			print ('Connection Error', url + cursor, str(sys.exc_info()[0]), str(e))
			return []
		except UnicodeDecodeError as e:
			print ('Encoding exception', url + cursor, str(sys.exc_info()[0]), str(e))
			return []
	return results
	
def getTotal(url):
	print("Getting total...")
	try:
		response = requests.get(url)
		result = response.json()
		return result['totalResults']
	except Exception as e:
		print ('Failed to retrieve total', url, str(sys.exc_info()[0]), str(e))
		print ('Dumping result...')
		print (str(result))
		return []

def processSet(set):
	global urlPattern, ids
	results = []
	# title, description, rights_statement, media_type, credit, credit_url, source_url
	if 'items' in set:
		items = set['items']
		for item in items:
			result = {}
			if item['id'] in ids:
				print("double: http://www.europeana.eu/portal/record%s.html" % item['id'])
				continue
			else: 
				ids.append(item['id'])
			result['id'] = 'europeana_' + item['id'].replace('/','__')
			''' NO LINK TO INSTITUTION NECCESARY?
			result['institution_link'] = ""
			if 'edmIsShownAt' in item:
				if isinstance(item["edmIsShownAt"], (list, tuple)):
					result['institution_link'] = item["edmIsShownAt"][0]
				else:
					result['institution_link'] = item["edmIsShownAt"]
			'''
			if urlPattern.match(item['edmIsShownBy'][0]) is not None:
				result['source_url'] = item['edmIsShownBy'][0]
			else:
				print('missing url, skipping %s' % item['id'])
				continue
			result['rights_statement'] = item['rights'][0]
			#result['source'] = "http://www.europeana.eu/portal/record" + item['id'] + ".html"
			result['title'] = ' | '.join(item["title"])
			result['credit'] = ""
			if 'dcCreator' in item:
				if isinstance(item["dcCreator"], (list, tuple)):
					result['credit'] = ' | '.join(item["dcCreator"])
				else:
					result['credit'] = item["dcCreator"]
			
			result['description'] = ""
			if 'dcDescription' in item:
				if isinstance(item["dcDescription"], (list, tuple)):
					result['description'] = ' ß| '.join(item["dcDescription"])
				else:
					result['description'] = item["dcDescription"]
			results.append(result)	
	return results

# PROCESSING PART

data = []

print('Starting Querying...')
total = getTotal(api+query+ '&cursor=*')
print("Total: %i" % total)
data = data + getResults(api+query,'&cursor=*', total)
if data != []:
	print('Query completed.')
print('Trying to store Data...')
try:
	with open('output.json', 'w') as outfile:
		json.dump(data, outfile)
	outfile.close()
	print('Data written to json')
except:
	print("ERROR: Storage of result failed.")
