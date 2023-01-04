"""
Manage and report on the SAEF dataverse collection.
"""
import pandas as pd
import pyDataverse
import requests
import saef

class SAEFCollection:
    """
    Methods for managing and reporting on a SAEF collection.

    Methods
    -------
    initialize : pyDataverse api, str
        Initialize the SAEFCollection instance.
    get_collection_contents :
        Get metadata about the contents of the collection.
    create_dataset_inventory : 
        Create an inventory of metadata about datasets in the collection.
    create_datafile_inventory : 
        Create an inventory of metadata about datafiles in the collection.
    destroy_dataset : pyDataverse api, string
        Delete a dataset from the collection.
    initd : 
        Inititalization status of instance.
    """
    def __init__(self):
        """
        Class constructor.
        """
        # is instance initialized?
        self._initd = False
        # url for the dataverse collection
        self._dataverse_collection_url = None
        # contents of the dataverse collection
        self._contents = {}

    def __process_geospatial_metadata(self, geo_md):
        """
        Private: Process geospatial metadata returned by API call.
        Called by: SAEFCollection::create_saef_dataset_inventory.

        Return
        ------
        dict
        """
        elements = {}
        if (not geo_md):
            return {}
        fields = geo_md.get('fields')
        if (fields and type(fields) == list):
            geographic_coverage = fields[0]
            if (type(geographic_coverage) is dict):
                fields = geographic_coverage.get('value')
                for field in fields:
                    for key in field.keys():
                        if (not elements.get(key)):
                            elements[key] = []
                        value = field[key].get('value')
                        elements[key].append(value)
            for element in elements:
                value = elements.get(element)
                if (type(value[0]) is str):
                    elements[element] = value[0]        
                else:
                    elements[element] = ';'.join(value[0])
        return elements

    def __get_dataset_metadata(self, api, dataset_id):
        """
        Private: Get the dataset metadata from the dataverse collection.

        Return
        ------
        dict
        """
        # handle input errors
        if (not api) or (not dataset_id):
            return {}
        
        # set up the request
        import requests
        # get the base url
        base_url = api.base_url
        # get the api token
        api_token = api.api_token
        # create the headers
        headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
        # create the request url
        request_url = '{}/api/datasets/{}/versions'.format(base_url,dataset_id)   
        # call the requests library using the request url
        response = requests.get(request_url, headers=headers)
        
        # handle responses
        status = response.status_code
        if (not (status >= 200 and status < 300)):
            print('TEST: Error - failed to get dataset metadata for dataset: {}'.format(dataset_id))
            return {}
        else:
            # get response data
            data = response.json().get('data')
            
            # metadata
            md = {}
                    
            # add additional metadata
            md['create_time'] = data[0].get('createTime')
            md['dataset_id'] = data[0].get('datasetId')
            md['dataset_pid'] = data[0].get('datasetPersistentId')
            return md
        
    def __get_contents(self, api):
        """
        Private: Get the contents of the SAEF dataverse collection.

        Return
        ------
        dict
        """
        # handle invalid input
        if (not api) or (not self._dataverse_collection_url):
            return {}
        # set up the request
        import requests
        # get the base url
        base_url = api.base_url
        # get the api token
        api_token = api.api_token
        # create the headers
        headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
        # create the request url
        request_url = '{}/api/dataverses/{}/contents'.format(base_url, self._dataverse_collection_url)
        # call the requests library using the request url
        response = requests.get(request_url, headers=headers)
        # handle responses
        status = response.status_code
        # handle errors
        if (not (status >= 200 and status < 300)):
            print('TEST: Error - failed to get inventory for collection: {} {}'.format(self._dataverse_collection_url,
            response.json()))
            return {}
        
        # get the inventory results
        results = response.json().get('data')
        
        # build the contents dict
        contents = {}
        
        # gather metadata about each dataset's files
        for result in results:
            # get the dataset metadata
            did = result.get('id')
            dmd = self.__get_dataset_metadata(api, did)
            
            # get the dataset's persistent id
            pid = 'doi:{}/{}'.format(result.get('authority'),result.get('identifier'))
            # create headers
            h = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
            # create request url for metadata about the dataset's files
            rurl = '{}/api/datasets/:persistentId/?persistentId={}'.format(base_url, pid)
            response = requests.get(rurl, headers=h)
            # add the file metadata to the contents       
            contents[pid] = {}
            contents[pid]['dataset'] = dmd
            contents[pid]['files'] = response.json().get('data')
            
        return contents

    def __get_datafiles_info(self, files):
        """
        Private: 
        """
        file_info = []
        for file in files:
            info = {}
            info['filename'] = file.get('dataFile').get('filename')
            info['description'] = file.get('dataFile').get('description')
            info['id'] = file.get('dataFile').get('id')
            info['originalFileFormat'] = file.get('dataFile').get('originalFileFormat')
            info['contentType'] = file.get('dataFile').get('contentType')
            info['creationDate'] = file.get('dataFile').get('creationDate')       
            info['originalFileName'] = file.get('dataFile').get('originalFileName')
            info['filesize'] = file.get('dataFile').get('filesize')
            categories = file.get('categories')
            cats = self.__get_datafile_categories(categories)
            if (cats):
                for cat in cats.keys():
                    info[cat] = cats.get(cat)
            file_info.append(info)
        return file_info

    def __get_datafile_categories(self, categories):
        """
        Private: 
        """
        ret = {}
        if (not categories):
            return ret
        for category in categories:
            tokens = category.split(':')
            # if the category doesn't have a colon (e.g., Data or Documentation)
            if (tokens[0] == category):
                cat = category
                val = 'True'
            else:
                cat = tokens[0]
                val = tokens[1]
            if (not ret.get(cat)):
                ret[cat] = []
            ret[cat].append(val)
            
        for key in ret.keys():
            value = ret.get(key)
            ret[key] = ';'.join(value)
        return ret

    def initialize(self, api, collection_url):
        """
        Initialize the instance.

        Parameters
        ----------
        api : pyDataverse api
        collection_url : str

        Return
        ------
        bool
        """
        # set the collection url
        self._dataverse_collection_url = collection_url
        # get the contents of the collection
        self._contents = self.__get_contents(api)
        if (not self._contents):
            print('TEST: Error - empty dictionary')
            return False
        return True
        
    def create_dataset_inventory(self):
        """
        Create an inventory of metadata about datasets in the collection.

        Return
        ------
        DataFrame
        """
        inventory = {}
        if (self._contents == None):
            return None
        
        for key in self._contents.keys():
            inventory[key] = {}
            dataset_metadata = self._contents.get(key).get('dataset')
            for element in dataset_metadata.keys():
                inventory[key][element] = dataset_metadata.get(element)
            
            # get metadata blocks
            metadata_blocks = self._contents.get(key).get('files').get('latestVersion').get('metadataBlocks')
            
            # process citation metadata
            citation = metadata_blocks.get('citation')
            fields = citation.get('fields')
            for field in fields:
                # selectively assign a few metadata elements
                if (field.get('typeName') == 'kindOfData'):
                    value = field.get('value')
                    inventory[key]['kind_of_data'] = ';'.join(value)
                if (field.get('typeName') == 'originOfSources'):
                    inventory[key]['origin_of_sources'] = field.get('value')
                if (field.get('typeName') == 'subject'):
                    value = field.get('value')
                    inventory[key]['subject'] = ';'.join(value)
                
            # process saef custom metadata
            saef = metadata_blocks.get('customSAEF')
            if (saef):
                saef_md = {}
                fields = saef.get('fields')
                for field in fields:
                    name = field.get('typeName')
                    if (not saef_md.get(name)):
                        saef_md[name] = []
                    value = field.get('value')
                    if value:
                        saef_md[name].append(value)
                for val in saef_md.keys():
                    value = saef_md.get(val)
                    if (type(value[0]) is str):
                        inventory[key][val] = value[0]
                        #saef_md[key] = value[0]
                    else:
                        inventory[key][val] = ';'.join(value[0])
                
            # process geospatial metadata
            geospatial_metadata = metadata_blocks.get('geospatial')
            geo_md = self.__process_geospatial_metadata(geospatial_metadata)
            for geo in geo_md.keys():
                inventory[key][geo] = geo_md.get(geo)
            
            # collect file statistics
            file_metadata = self._contents.get(key).get('files')
            files = file_metadata.get('latestVersion').get('files')
            inventory[key]['numFiles'] = len(files)
            files_info = self.__get_datafiles_info(files)
            content_types_count = {}
            for info in files_info:
                content_t = info.get('contentType')
                if (content_types_count.get(content_t)):
                    content_types_count[content_t] = content_types_count[content_t] + 1
                else:
                    content_types_count[content_t] = 1
            inventory[key]['contentTypesCount'] = content_types_count
            citation_metadata = file_metadata.get('latestVersion').get('metadataBlocks').get('citation')
            fields = citation_metadata.get('fields')
            for field in fields:
                if (field.get('typeName') == 'title'):
                    inventory[key]['title'] = field.get('value')
                if (field.get('typeName') == 'dsDescription'):
                    desc = field.get('value')
                    ds_description_value = desc[0].get('dsDescriptionValue').get('value')
                    inventory[key]['dsDescription'] = ds_description_value            
        
        # write each item in the inventory to a records
        records = []
        for key in inventory:
            record = {}
            record['dataset_doi'] = key
            for item in inventory[key].keys():
                record[item] = inventory[key].get(item)
            content_types = inventory.get(key).get('contentTypesCount')
            for content_type in content_types.keys():
                count = content_types.get(content_type)
                record[content_type] = count
            records.append(record)
            
        # write the records to a dataframe        
        return pd.DataFrame.from_records(records,index=None)

    def create_datafile_inventory(self):
        """
        Create an inventory of metadata about datafiles in the collection.

        Return
        ------
        DataFrame
        """
        records = []
        if (not self._contents):
            return None
        
        for key in self._contents.keys():
            file_metadata = self._contents.get(key).get('files')
            files = file_metadata.get('latestVersion').get('files')
            datafiles = self.__get_datafiles_info(files)
            
            for datafile in datafiles:
                record = {}
                record['dataset_doi'] = key
                for attribute in datafile.keys():
                    record[attribute] = datafile[attribute]
                records.append(record)
        
        return pd.DataFrame.from_records(records,index=None)

    def destroy_dataset(self, api, dataset_pid):
        """
        Delete a dataset from the collection.
        This is a destructive, permanent action that cannot be undone.

        Parameters
        ----------
        api : pyDataverse api
        dataset_pid : str
            DOI of dataset to delete from the collection

        Return 
        ------
        bool
        """
        if ((not api) or
            (not dataset_pid)):
            return False

        # destroy dataset using pyDataverse api option
        response = api.destroy_dataset(dataset_pid, is_pid=True, auth=True)
        status = response.status_code
        # handle errors
        if (not (status >= 200 and status < 300)):
            print ('TEST: Error - failed to delete dataset: {}'.format(dataset_pid))
            return False
        else:
            return True

    def get_collection_contents(self):
        """
        Get metadata about the contents of the collection.

        Return
        ------
        dict
        """
        return self._contents

    def initd(self):
        """
        Get the initialization status for the istance.
        """
        return self._initd

# end file