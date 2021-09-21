from elasticsearch import Elasticsearch, helpers
from collections import deque
from lxml import etree
import datetime


def gendata():
    tree = etree.parse(r'pubmed21n0670.xml.gz')
    root = tree.getroot()
    entry = {}

    #PMID, _id
    entry['PMID'] = []    
    for id in root.iter('PMID'):
      entry['PMID'].append(id.text)
    #entry['_id'] = entry['PMID'].copy()
    #Date when the document published on Medline
    #Gets the years
    years = []
    for date in root.findall(
            "./PubmedArticle/MedlineCitation/DateCompleted/Year"):
      years.append(date.text)
    #Gets the months
    months = []
    for date in root.findall(
            "./PubmedArticle/MedlineCitation/DateCompleted/Month"):
      months.append(date.text)
    #Gets the days
    days = []
    for date in root.findall(
            "./PubmedArticle/MedlineCitation/DateCompleted/Day"):
      days.append(date.text)
    #Zip them together and add to datetime object in YYYY-MM-DD format
    entry['PublishDate'] = []
    for (a,b,c) in zip(years,months,days):
      x= datetime.datetime(int(a), int(b), int(c))
      entry['PublishDate'].append(x.strftime("%Y-%m-%d"))

    #List of the MeshHeading unique IDs
    entry['MeshIDs'] = []
    for mesh in root.iter('DescriptorName'):
      entry['MeshIDs'].append(mesh.attrib['UI'])
    
    
    
    #Title of the article
    entry['Title'] = []
    for title in root.iter('ArticleTitle'):
      entry['Title'].append(title.text)

    #Authors of the article
    entry['Authors'] = []
    LastName = []
    ForeName = []
    for author in root.iter('AuthorList'):
        temp = []
        temp2 = []
        for tag in author:
            for item in tag:
              if item.tag == "LastName":
                temp.append(item.text)
              if item.tag == "ForeName":
                temp2.append(item.text)
        LastName.append(temp)
        ForeName.append(temp2)
    #flatten
    first = [x for l in ForeName for x in l]
    last = [x for l in LastName for x in l]
    for a,b in zip(first,last):
      entry['Authors'].append(f'{a} {b}')

    #Abstract text
    entry['Abstract'] = []
    for abstract in root.iter('AbstractText'):
        entry['Abstract'].append(abstract.text)
        if abstract.text:
          entry['Abstract'].append("No abstract")


    #List of keywords
    entry['Keywords'] = []
    for key in root.iter("Keyword"):
      entry['Keywords'].append(key.text)

    #Journal name
    entry['Journal'] = []
    for title in root.iter('Title'):
        entry['Journal'].append(title.text)

    #Uploader 
    entry['Uploader'] = "Bryan Nix"
    
      
    yield entry['PMID']
    yield entry['PMID']
    yield entry["PublishDate"]
    yield entry["MeshIDs"]
    yield entry["Title"]
    yield entry['Authors']
    yield entry['Abstract']
    yield entry['Keywords']
    yield entry['Journal']
    yield entry['Uploader']
      

if __name__ == '__main__':
    user = "elastic"
    passw = 'iYYX96TPlAJ000UJ0vqa'
    index_name = 'pubmed2021'   
    output = gendata()
    for i in output:
     print(i) 
    #es = Elasticsearch(hosts=['10.80.34.86:9200'], http_auth=(user, passw))
    #deque(helpers.parallel_bulk(es, gendata(), index=index_name), maxlen=0)
    #es.indices.refresh()
