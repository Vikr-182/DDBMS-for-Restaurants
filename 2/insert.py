import csv

csvfile = open('given.csv')

spamreader = csv.reader(csvfile)


relationlist = []
fraglist = []
bigcolumnlist = [] 


tableset = set()
fragset = set()
columnset = set ()


count = 0 
for row in spamreader:

    if count == 0 :
        count+= 1
        continue

    count +=1 
    fragname = row[0]
    fragtype = row[1]
    allocated_site = row[2]
    condition = row[3]
    columnlist = row[4].split(',')
    tablename = columnlist[0].split('.')[0].strip()
    
    if fragtype == 'H' or fragtype == 'DH' :

        if tablename not in tableset:
            newdict = {}

            newdict['idTable'] = len(tableset)
            newdict['TableName'] = tablename
            newdict['ColumnsNo'] = len(columnlist)
            newdict['FragmentationType'] = fragtype
            newdict['FragCount'] = 1
            newdict['collist'] = columnlist
            relationlist.append(newdict)

            print ("columnlist ", columnlist)

            for iname in columnlist:
                newcoldict = {}
                newcoldict['ColumnID'] = len(columnset)
                newcoldict['ColumnType'] = 'Varchar (50)'
                newcoldict['ColumnName'] = iname.split('.')[1]
                newcoldict['TableID'] = len(tableset)
                bigcolumnlist.append(newcoldict)
                print ((iname,len(tableset),len(columnset)))
                columnset.add((iname,len(tableset)))


            tableset.add(tablename)

        else :
            for i in relationlist:
                if i['TableName'] == tablename:
                    i['FragCount'] += 1

        if fragtype == 'H' :
            newfragdict = {}
            newfragdict['FragmentationID'] = len(fragset)
            newfragdict['FragmentationCondition'] = condition 
            newfragdict['SiteId'] = allocated_site

            fragset.add(fragname)
            for i in relationlist:
                if i['TableName'] == tablename:
                    newfragdict['RelationId'] = i['idTable']

            fraglist.append(newfragdict)

        if fragtype == 'DH' :
            newfragdict = {}
            newfragdict['FragmentationID'] = len(fragset)
            newfragdict['FragmentationCondition'] = condition
            newfragdict['SiteId'] = allocated_site
            fragset.add(fragname)


            for i in relationlist:
                if i['TableName'] == tablename:
                    newfragdict['RelationId'] = i['idTable']
            fraglist.append(newfragdict)


    elif fragtype == 'V':
        if tablename not in tableset:
            newdict = {}
            newdict['idTable'] = len(tableset)
            newdict['TableName'] = tablename
            newdict['ColumnsNo'] = len(columnlist)
            newdict['FragmentationType'] = fragtype
            newdict['FragCount'] = 1
            newdict['collist'] = columnlist
            relationlist.append(newdict)


            for iname in columnlist:
                newcoldict = {}
                newcoldict['ColumnID'] = len(columnset)
                newcoldict['ColumnType'] = 'Varchar (50)'
                newcoldict['ColumnName'] = iname.split('.')[1]
                newcoldict['TableID'] = len(tableset)
                bigcolumnlist.append(newcoldict)
                print ((iname,len(tableset),len(columnset)))
                columnset.add((iname,len(tableset)))

            tableset.add(tablename)
            print (tableset)

        else :
            print ("AAYA TOH HU")


            for ij in relationlist:
                if ij['TableName'] == tablename:
                    ij['FragCount'] += 1
                    ij['ColumnsNo'] = len(set(columnlist+ij['collist']))
                    print (columnlist,ij['collist'],columnlist+ij['collist'])
                    ij['collist'] = list(set(columnlist+ij['collist']))

                    tabid = ij['idTable']

                    for pkk in columnlist :
                        if (pkk,tabid) not in columnset:
                            print ("LOLOLOLOL", pkk)
                            newcoldict = {}
                            newcoldict['ColumnID'] = len(columnset)
                            newcoldict['ColumnType'] = 'Varchar (50)'
                            newcoldict['ColumnName'] = pkk.split('.')[1]
                            newcoldict['TableID'] = tabid
                            bigcolumnlist.append(newcoldict)
                            print ((pkk,tabid,len(columnset)))
                            columnset.add((pkk,tabid))


        newfragdict = {}
        newfragdict['FragmentationID'] = len(fragset)
        templist=[]
        for kk in columnlist :
            templist.append(kk.split('.')[1])

        newfragdict['FragmentationCondition'] = templist
        newfragdict['SiteId'] = allocated_site
        for i in relationlist:
                if i['TableName'] == tablename:
                    newfragdict['RelationId'] = i['idTable']
        fraglist.append(newfragdict)
        fragset.add(fragname)

    else : 

        if tablename not in tableset:
            newdict = {}

            newdict['idTable'] = len(tableset)
            newdict['TableName'] = tablename
            newdict['ColumnsNo'] = len(columnlist)
            newdict['FragmentationType'] = fragtype
            newdict['FragCount'] = 1
            newdict['collist'] = columnlist
            relationlist.append(newdict)

            print ("columnlist ", columnlist)

            for iname in columnlist:
                newcoldict = {}
                newcoldict['ColumnID'] = len(columnset)
                newcoldict['ColumnType'] = 'Varchar (50)'
                newcoldict['ColumnName'] = iname.split('.')[1]
                newcoldict['TableID'] = len(tableset)
                bigcolumnlist.append(newcoldict)
                print ((iname,len(tableset),len(columnset)))
                columnset.add((iname,len(tableset)))


            tableset.add(tablename)
            newfragdict = {}
            newfragdict['FragmentationID'] = len(fragset)
            newfragdict['FragmentationCondition'] = condition 
            newfragdict['SiteId'] = allocated_site

            fragset.add(fragname)
            for i in relationlist:
                if i['TableName'] == tablename:
                    newfragdict['RelationId'] = i['idTable']

            fraglist.append(newfragdict)

        
    

    print (', '.join(row) )

print (relationlist)

vertidlist = []
for i in relationlist:
    if i['FragmentationType'] == 'V':
        vertidlist.append(i['idTable'])

for i in fraglist:
    if i['RelationId'] in vertidlist:
        scolumnlist = i['FragmentationCondition']
        newnumlist = []
        for j in bigcolumnlist:
            if j['TableID'] == i['RelationId'] and j['ColumnName'] in scolumnlist:
                print ('appending this column', j)
                newnumlist.append(str(j['ColumnID']))
        i['FragmentationCondition'] = (',').join(newnumlist)



bigjson = {'RELATIONS':relationlist,'FRAGMENTATION':fraglist,'COLUMNS':bigcolumnlist}

siteslist =  [
        {
            "idSite": 1,
            "User": "user",
            "ip": "10.3.5.211",
            "password": "iiit123",
            "HostName": "CP5"
        },
        {
            "idSite": 2,
            "User": "user",
            "ip": "10.3.5.208",
            "password": "iiit123",
            "HostName": "CP6"
        },
        {
            "idSite": 3,
            "User": "user",
            "ip": "10.3.5.204",
            "password": "iiit123",
            "HostName": "CP7"
        },
        {
            "idSite": 4,
            "User": "user",
            "ip": "10.3.5.205",
            "password": "iiit123",
            "HostName": "CP8"
        }
    ]

bigjson['SITES'] = siteslist

for i in bigjson['RELATIONS']:
    del i['collist']



import json
out_file = open("new_schema.json", "w")

json.dump(bigjson,out_file,indent=6)



