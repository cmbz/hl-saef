"""
SAEF Classes

Python classes supporting Houghton Library Slavery, Abolition, Emancipation, 
and Freedom (SAEF) digital project content.

https://curiosity.lib.harvard.edu/slavery-abolition-emancipation-and-freedom
"""

import configparser
import copy
import datetime
import ddu # local: dataverse direct upload module
import json
import lcd # local: library collections as data module
import mimetypes
import os
import pandas as pd
import pyDataverse
import requests
import time

class SAEFProjectConfig:
    """
    Reader for SAEF Project processing .ini configuration files.
    Depends upon the configparser module.
    
    Methods
    -------
    read_ini : str
        Read and parse the .ini config file.
    get_options : void
        Get the options from the .ini file.
    get_sections : void
        Get the sections from the .ini file.
    initd : void
        Get the initialization status of the instance.
    initialize_dataverse_api_log : void
        Initialize the Dataverse API log for later use.
    """
    
    def __init__(self):
        """
        Class constructor.
        """

        # is instance initialized?
        self._initd = False
        # init file path
        self._ini_file = None
        # configuration parser
        self._configparser = configparser.ConfigParser()
        # configure the parser with sections
        self._configparser.sections()
        # required sections and options, must be present, even if blank
        # note: update this table whenever you update the key/value directives in the [project].ini file
        self._options = {
            # inventory options
            'inventory':{
                'inventory_filename':None,   
                'inventory_data_directory':None,
            },
            # digital object options
            'digital_object':{
                'digital_object_relationships_directory':None,
                'digital_object_pds_relationships':None,
                'digital_object_msft_relationships':None,
                'digital_object_ocr_relationships':None,                
            },
            # dataset options
            'dataset':{
                'dataset_author':None,
                'dataset_author_affiliation':None,
                'dataset_contact_name':None,
                'dataset_contact_affiliation':None,
                'dataset_contact_email':None,
                'dataset_metadata':None,   
                'dataset_subject':None,
            },
            # dataverse options
            'dataverse':{               
                'dataverse_api_logfile':None,
                'dataverse_collection_url':None,
                'dataverse_installation_url':None,
                'dataverse_api_key':None         
            }
        }
        # set valid sections member
        self._sections = self._options.keys()

    def read_ini(self, filename):
        """
        Read and parse the .ini config file.

        Parameter
        ---------
        filename : str

        Raises
        ------
        ValueError
            Missing required .ini section.
            Missing required .ini option.

        Return
        ------
        bool
        """
        # read the ini file
        self._configparser.read(filename)
        # set filename member
        self._ini_file = filename
        # get the sections from the ini file
        sections = self._configparser.sections()
        
        # confirm that all required sections are present
        for section in self._sections:
            # if the section is missing, return False
            if self._configparser.has_section(section) == False:
                raise ValueError('SAEFProjectConfig::read_ini: Error - missing required section: {}'.format(section))
            
            # get the section options from the ini file
            section_options = self._configparser.options(section)
            
            # confirm that each section option is present
            for option in self._options.get(section):
                # check for the option
                if (self._configparser.has_option(section,option) == False):
                    raise ValueError('SAEFProjectConfig::read_ini: Error - missing required {} option: {}'.format(section, option))
                else:
                    self._options[section][option] = self._configparser[section][option]
        self._initd = True
        return True
    
    def get_options(self):
        """
        Get the options from the .ini file.
        
        Return
        ------
        dict
        """
        return(self._options)
    
    def get_sections(self):
        """
        Get the sections from the .ini file.

        Return
        ------
        list
        """
        return (self._sections)
    
    def initd(self):
        """
        Get the initialization status of the instance.

        Return
        ------
        bool
        """
        return self._initd
    
    def initialize_dataverse_api_log(self):
        """
        Initialize the Dataverse API log for later use.

        Catches
        -------
        File exceptions

        Return
        ------
        bool
        """
        # get the logfile
        logfile = self._options.get('dataverse').get('dataverse_api_logfile')
        # create the log if it doesn't exist
        from os.path import exists
        if (not exists(logfile)):
            fp = open(logfile, 'w+')
            fp.close()
        # otherwise, add the header
        try:
            with open(logfile, 'r+') as fp:
                # create the header
                header = '{}\t{}\t{}\t{}\t{}\n'.format('time', 'function', 'api operation', 'status', 'message')
                # check presence of header
                hdr = fp.readline()
                if (hdr != header):
                    # write the header
                    fp.write(header)
                # close the file
                fp.close()
            return True
        except:
            return False

class MSFTInventory (lcd.FileInventory):
    """
    Subclass of FileInventory to manage metadata about inventories of Microsoft Transcription files.
    Valid file formats are: MSFT_IMG, MSFT_TXT, and mSFT_JSON.

    Methods
    -------
    from_dataframe : DataFrame
        Initialize an MSFT inventory from a DataFrame.
    from_file : str
        Initialize an MSFT inventory from a file.
    get_msft_img_files : void
        Get the MSFT image file information.
    get_msft_json_files : void
        Get the MSFT JSON file information.
    get_msft_txt_files : void
        Get the MSFT TXT file information.
    get_files : void
        Get DataFrame of information about all MSFT files.
    initd : void
        Get the initialization status of the instance.

    """
    # method: constructor: __init__
    def __init__(self):
        """
        Class constructor
        """
        # init the base class
        lcd.FileInventory.__init__(self)

        # valid msft formats
        self._msft_formats = {'msft_img':'MSFT-PNG',
                              'msft_json':'MSFT-JSON',
                              'msft_txt':'MSFT-TXT'}
        
        # init status
        self._initd = False
    
    # method: __validate_msft
    # description: validate the microsoft automatic transcription dataframe
    # return: MSFT dataframe on success or raises ValueError or TypeError on failure
    def __validate(self, dataframe):
        """
        Private: Validate the microsoft automatic transcription dataframe.

        Parameter
        ---------
        dataframe : DataFrame

        Raises
        ------
        TypeError
            Input is not a DataFrame
        ValueError
            Input DataFrame cannot be empty
            Input DataFrame must contain at most one owner-supplied name

        Return
        ------
        bool
        """
        # input must be DataFrame
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('MSFTInventory::__validate - input must be of type DataFrame')        
        
        # dataframe cannot be empty
        if (dataframe.empty == True):
            raise ValueError('MSFTInventory::__validate - DataFrame cannot be empty')    
        
        # there can only be one owner-supplied name in the DataFrame
        unique_osn = dataframe['object_osn'].unique()
        if (len(unique_osn) > 1):
            raise ValueError('MSFTInventory::__validate: DataFrame must contain no more than one owner-supplied name')  
            
        # inspect the contents of the dataframe
        fi = lcd.FileInventory()
        status = fi.from_dataframe(dataframe)
        if (status == False):
            # return an empty dataframe
            return pd.DataFrame()
        
        # get the MST-related files from the inventory
        msft_img = fi.get_files('file_format', self._msft_formats.get('msft_img'))
        msft_json = fi.get_files('file_format', self._msft_formats.get('msft_json'))
        msft_txt = fi.get_files('file_format', self._msft_formats.get('msft_txt'))
        
        # if there is a count mismatch for mst files
        # note: this may be okay, depending upon the situation, the function caller should decide
        msft_img_count = len(msft_img)
        msft_json_count = len(msft_json)
        msft_txt_count = len(msft_txt)
        if ((msft_img_count != msft_json_count) or
            (msft_img_count != msft_txt_count) or
            (msft_txt_count != msft_json_count)):
            print('MSFTInventory::__validate: Warning, mismatch in count of Microsoft transcription files in this inventory')    
            # return an empty dataframe
            return pd.DataFrame()
        
        # if there are no mst files 
        # note: this may be okay, depending upon the situation, the function caller should decide
        if ((msft_img_count == 0) and
            (msft_json_count == 0) and
            (msft_txt_count == 0)):
            print('MSFTInventory::__validate: Warning, no Microsoft transcription files in this inventory')
            # return an empty dataframe
            return pd.DataFrame()
        
        # all error conditions have been passed
        # concatenate the three msft dataframes and return
        return pd.concat([msft_img,msft_json,msft_txt])
        
    def from_dataframe(self, inventory_df):
        """
        Initialize an MSFT inventory from a DataFrame.

        Parameter
        ---------
        dataframe : DataFrame

        Catches
        -------
            TypeError
            ValueError

        Return
        ------
        bool
        """
        # try to validate the inventory dataframe
        try:
            # validate the inventory_df
            self._inventory_df = self.__validate(inventory_df)
        # catch exceptions
        except TypeError:
            # inventory is not a dataframe
            return False
        except ValueError:
            # inventory is missing fields or values
            return False            
        else:
            self._initd = True
            # return True on success
            return True

    def from_file(self, filename):
        """
        Initialize an MSFT inventory from a file.

        Parameter
        ---------
        filename : str

        Catches
        -------
        TypeError
        ValueError

        Return
        ------
        bool
        
        """
        # filename must not be empty string
        if (not filename):      
            return False
        
        # read the inventory file
        inventory_df = pd.read_csv(filename, sep=',',header=0)
        return self.from_dataframe(inventory_df)
    
    def get_msft_img_files(self):
        """
        Get the MSFT image file information.
        
        Return
        ------
        DataFrame
        """
        format = self._msft_formats.get('msft_img')
        files = self._inventory_df[self._inventory_df['file_format'] == format]
        return files
    
    def get_msft_json_files(self):
        """
        Get the MSFT JSON file information.

        Return
        ------
        DataFrame
        """
        files = self._inventory_df[self._inventory_df['file_format'] == self._msft_formats.get('msft_json')]
        return files
    
    def get_msft_txt_files(self):
        """
        Get the MSFT TXT file information

        Return
        ------
        DataFrame
        """
        files = self._inventory_df[self._inventory_df['file_format'] == self._msft_formats.get('msft_txt')]
        return files        

    def get_files(self):
        """
        Get DataFrame of information about all MSFT files.

        Return
        ------
        DataFrame
        """
        return self._inventory_df
    
    def initd(self):
        """
        Get the initialization status of the instance.

        Return
        ------
        bool
        """
        return self._initd

class SAEFDigitalObject(lcd.PDSDocument):
    """
    Subclass of PDSDocument supporting information about SAEFDigitalObjects.
    Inventory must contain inventory fields: mms_id and object_tags.
    Combines PDSDocument, MSFTInventory (Microsoft hand-writing recognition software), and OCRInventory files. 
    Instance is not required to have Microsoft transcription files or OCR files. 

    Methods
    -------
    from_dataframe : DataFrame
        Initialize a SAEFDigitalObject from a DataFrame.
    from_file : str
        Initialize a SAEFDigitalObject from a *.csv file.
    get_mets_file : void
        Get the PDSDocument METS file information.
    get_image_files : void
        Get the PDSDocument image file information.
    get_msft_img_files : void
        Get the MSFT image file information.
    get_msft_json_files : void
        Get the MSFT JSON file information.
    get_msft_txt_files : void
        Get the MSFT TXT file information.
    get_files : void
        Get DataFrame of information about all MSFT files.
    get_pds_relationships : void
        Get the DataFrame containing relationships between the METS and image files.
    get_msft_relationships : void
        Get the DataFrame containing relationships between the MSFT files and image files.
    get_ocr_relationships : void
        Get the DataFrame containing relationships between the OCR files and image files.
    write_relationships : str, str (ocr|pds|msft)
        Write a named relationships DataFrame to a file.        
    initd : void
        Get the initialization status of the instance.
    """

    def __init__(self):
        """
        Class constructor.
        """
        lcd.PDSDocument.__init__(self)

        # dataframe for pds relationships
        self._pds_relationships = None
        # dataframe for msft relationships
        self._msft_relationships = None
        # dataframe for ocr relationships
        self._ocr_relationships = None
        # pdsdocument instance
        self._pdsdocument = None
        # msft inventory
        self._msft_fi = None
        # ocr inventory
        self._ocr_fi = None
        # saef digital object metadata (has extra elements)
        self._saef_metadata = None
        # instance is/not initialized
        self._initd = False
    
    def __validate(self, pdsdocument):
        """
        Private: Validate the pdsdocument dataframe; must have additional mms_id and object_tags fields.

        Parameter
        ---------
        pdsdocument : PDSDocument

        Return
        ------
        bool
        """
        # get the pdsdocument dataframe
        df = pdsdocument.get_files()
        
        # test saef-specific column values
        # mms_id (manuscript id) - column must exist; it may be empty
        if (not 'mms_id' in df):
            return False
        
        # object_tags - column must exist and contain values
        if (not 'object_tags' in df):
            return False

        return True
    
    def __define_pds_relationships(self, pdsdocument):
        """
        Private: Assign relationships between JP2 and METS XML files for PDS Documents.
        Order dependent; called only after PDSDocument has been created.

        Parameter
        ---------
        pdsdocument : PDSDocument

        Return
        ------
        DataFrame

        Relationship DataFrame Format
        -----------------------------
        filename_source : str
            Name of the source file.
        filename_target : str
            Name of the target/related file.
        relationship : str
            The type of relationship that holds between the two files. 
            Examples include belongs_to and contains
        source_file_format : str
            The format of the source file.
        target_file_format : str
            The format of the target file.
        """
        # create the relationships dataframe
        relationships = pd.DataFrame()
        # get the mets file
        mets = pdsdocument.get_mets_file()
        index = list(mets.index)[0]
        mets_filename = mets.at[index,'filename']
        mets_file_format = mets.at[index,'file_format']
        # get the image files
        images = pdsdocument.get_image_files()
        # populate the relationships dataframe
        for image in images.iterrows():
            image_filename = image[1].get('filename')
            image_file_format = image[1].get('file_format')
            # create an entry for the image file
            relationships = relationships.append({'filename_source':image_filename,
                                                  'source_file_format':image_file_format,
                                                  'relationship':'belongs_to',
                                                  'filename_target':mets_filename,
                                                  'target_file_format':mets_file_format},ignore_index=True)
                
            # create a reciprocal entry for the mets file
            relationships = relationships.append({'filename_source':mets_filename,
                                                  'source_file_format':mets_file_format,
                                                  'relationship':'contains',
                                                  'filename_target':image_filename,
                                                  'target_file_format':image_file_format},ignore_index=True)
        return relationships

    def __define_msft_relationships(self, pdsdocument, dataframe):
        """
        Private: Assign relationships between components.

        Parameters
        ----------
        pdsdocument : PDSDocument
        dataframe : DataFrame

        Return
        ------
        DataFrame

        Relationship DataFrame Format
        -----------------------------
        filename_source : str
            Name of the source file.
        filename_target : str
            Name of the target/related file.
        relationship : str
            The type of relationship that holds between the two files. Examples include `belongs_to` and `contains`
        source_file_format : str
            The format of the source file.
        target_file_format : str
            The format of the target file.
        """
        # create the relationships dataframe
        relationships = pd.DataFrame() 
        # if the dataframe is empty, return empty relationships table
        if (dataframe.empty):
            return relationships
        
        # get the image files
        images = pdsdocument.get_image_files()
        # create a dictionary keyed file osn
        md = {}
        for image in images.iterrows():
            osn = image[1].get('file_osn')
            filename = image[1].get('filename')
            file_format = image[1].get('file_format')
            md[osn] = {'filename':filename, 'file_format':file_format}
            
        # for each row in the msft dataframe
        for row in dataframe.iterrows():
            # get the file_osn
            file_osn = row[1].get('file_osn')
            # get the matching image file
            image_file = md.get(file_osn)
            # set variables
            source_filename = image_file.get('filename')
            source_file_format = image_file.get('file_format')
            target_filename = row[1].get('filename')
            target_file_format = row[1].get('file_format')
            
            # create an entry for the is-derived-from relationship between the transcript and its source jpg
            relationships = relationships.append({'filename_source':target_filename,
                                                  'source_file_format':target_file_format,
                                                  'relationship':'is_derived_from',
                                                  'filename_target':source_filename,
                                                  'target_file_format':source_file_format},ignore_index=True)
            
            # create an entry for the is-source-for relationship between the transcript and its source jpg
            relationships = relationships.append({'filename_source':source_filename,
                                                  'source_file_format':source_file_format,
                                                  'relationship':'is_source_for',
                                                  'filename_target':target_filename,
                                                  'target_file_format':target_file_format},ignore_index=True)
        # return the relationships dataframe
        return relationships
    
    def __define_ocr_relationships(self, pdsdocument, dataframe):
        """
        Private: Assign relationships between components

        Parameters
        ----------
        pdsdocument : PDSDocument
        dataframe : DataFrame

        Return
        ------
        DataFrame

        Relationship DataFrame Format
        ------------------------
        filename_source : str
            Name of the source file.
        filename_target : str
            Name of the target/related file.
        relationship : str
            The type of relationship that holds between the two files. Examples include `belongs_to` and `contains`
        source_file_format : str
            The format of the source file.
        target_file_format : str
            The format of the target file.
        """
        # create the relationships dataframe
        relationships = pd.DataFrame() 
        # if the dataframe is empty, return empty relationships table
        if (dataframe.empty):
            return relationships
        
        # get the image files
        images = pdsdocument.get_image_files()
        fi = lcd.FileInventory()
        fi.from_dataframe(images)
        images_md = fi.get_metadata(images)
        # create a dictionary keyed file osn
        md = {}
        for image in images_md:
            osn = image.get('file_osn')
            md[osn] = image
        
        # for each row in the msft dataframe
        for row in dataframe.iterrows():
            # get the file_osn
            file_osn = row[1].get('file_osn')
            # get the matching image file
            image_file = md.get(file_osn)
            # set variables
            source_filename = image_file.get('filename')
            source_file_format = image_file.get('file_format')
            target_filename = row[1].get('filename')
            target_file_format = row[1].get('file_format')
            
            # create an entry for the is-derived-from relationship between the ocr text and its source jpg
            relationships = relationships.append({'filename_source':target_filename,
                                                  'source_file_format':target_file_format,
                                                  'relationship':'is_derived_from',
                                                  'filename_target':source_filename,
                                                  'target_file_format':source_file_format},ignore_index=True)
            
            # create an entry for the is-source-for relationship between the ocr text and its source jpg
            relationships = relationships.append({'filename_source':source_filename,
                                                  'source_file_format':source_file_format,
                                                  'relationship':'is_source_for',
                                                  'filename_target':target_filename,
                                                  'target_file_format':target_file_format},ignore_index=True)
        # return the relationships dataframe
        return relationships

    def __check_metadata(self, metadata):
        """
        Private: Validate metadata parameter.

        Parameter
        ---------
        metadata : dict
            mms_id : str (e.g., '990091469160203941')
            object_tags : str (e.g., 'Created:1864;City:Richmond;State:Virginia')

        Return
        ------
        Bool
        """
        if (not isinstance(metadata,dict)):
            print('SAEFDigitalObject: Error - metadata must be of type dict.')
            return False
                        
        # get required keys
        mms_id = metadata.get('mms_id')
        object_tags = metadata.get('object_tags')

        # keys must be present
        if ((not mms_id) or
            (not object_tags)):
            print('SAEFDigitalObject: Error - metadata was supplied but is missing required keys.')
            return False

        # key values must be strings
        if ((not type(mms_id,str)) or
            (not type(object_tags,str))):
            print('SAEFDigitalObject: Error - etadata was supplied but is not string.')
            return False

        # success
        return True
    
    def from_dataframe(self, dataframe, metadata={}):
        """
        Initialize the SAEFDigitalObject using a dataframe.

        Parameters
        ----------
        dataframe : DataFrame 
            file_path : Column cannot have null/blank values
        metadata : dict, optional
            mms_id : str (e.g., '990091469160203941')
            object_tags : str (e.g., 'Created:1864;City:Richmond;State:Virginia')

        Catches
        -------
        TypeError
        ValueError

        Return
        ------
        bool
        """
        # initialize the status variable
        status = False

        # dataframe file_path cannot be empty
        if (dataframe['file_path'].isnull().values.any() == True):
            print('SAEFDigitalObject: Error - column: file_path cannot have null values')
            return False

        # apply metadata, if any, to dataframe
        if (bool(metadata)):
            # get key values, ignore if failed
            if (self.__check_metadata(metadata) == True):
                mms_id = metadata.get('mms_id')
                object_tags = metadata.get('object_tags')
                # apply keys to the dataframe
                dataframe['mms_id'] = mms_id
                dataframe['object_tags'] = object_tags

        # try to create a pdsdocument instance using the dataframe
        self._pdsdocument = lcd.PDSDocument()
        if (self._pdsdocument.from_dataframe(dataframe) == False):
            return False
        
        # validate PDSDocument for existence of SAEF-specific inventory fields
        # note: the fields may be empty
        status = self.__validate(self._pdsdocument)
        if (status == False):
            return False

        # check for related msft content
        self._msft_fi = MSFTInventory()
        status = self._msft_fi.from_dataframe(dataframe)
        msft_df = self._msft_fi.get_inventory()
        # if the operation failed, check for None vs. empty dataframe
        # None -> MSFT files present but not processed
        if ((status == False) and
            (msft_df == None)):
                print('SAEFDigitalObject::from_dataframe: Error - failed to process MSFT files in DataFrame')
                return False
        if ((status == False) and
            (msft_df.empty == True)):
            print('SAEFDigitalObject::from_dataframe: Warning - no MSFT files in DataFrame')

        # check for related ocr content
        self._ocr_fi = lcd.OCRInventory()
        status = self._ocr_fi.from_dataframe(dataframe)
        ocr_df = self._ocr_fi.get_inventory()
        # if the operation failed, check for None vs. empty dataframe
        # None -> OCR files present but not processed
        if ((status == False) and
            (ocr_df == None)):
                print('SAEFDigitalObject::from_dataframe: Error - failed to process OCR files in DataFrame')
                return False            
        
        # empty dataframe -> No OCR files present
        if (ocr_df.empty == True): 
                print('SAEFDigitalObject::from_dataframe: No OCR text files detected.') 

        # create relationships
        self._pds_relationships = pd.DataFrame()
        self._msft_relationships = pd.DataFrame()
        self._ocr_relationships = pd.DataFrame()    
        
        # define the relationships
        self._pds_relationships = self.__define_pds_relationships(self._pdsdocument)
        # if there is a msft dataframe, define relationships
        if (msft_df.empty == False):
            self._msft_relationships = self.__define_msft_relationships(self._pdsdocument,msft_df)
        # if there is a ocr dataframe, define relationshps
        if (ocr_df.empty == False):
            # note: uses the same method for msft relationships with a different argument
            self._ocr_relationships = self.__define_msft_relationships(self._pdsdocument,ocr_df)

        #
        # set saef-specific metadata
        #
        # get the pdsdocument metadata
        pdsmd = self._pdsdocument.get_metadata()
        # get the pdsdocument mets file
        mets = self._pdsdocument.get_mets_file()            
        # get the (integer) index of the mets file in the dataframe
        # for details, see: https://pandas.pydata.org/docs/reference/api/pandas.Index.html
        index = list(mets.index)[0]
        # set the saef metadata
        self._saef_metadata = copy.deepcopy(pdsmd)
        # populate the saef digital object-specific metadata
        self._saef_metadata['mms_id']= mets.at[index,'mms_id']
        self._saef_metadata['object_tags'] = mets.at[index,'object_tags']
          
        # the instance is initialized
        self._initd = True
        return True
    
    def from_file(self, filename, metadata={}):
        """
        Initialize a SAEFDigitalObject from a .csv file.

        Parameters
        ----------
        filename : str
        metadata : dict, optional
            mms_id : str (e.g., '990091469160203941')
            object_tags : str (e.g., 'Created:1864;City:Richmond;State:Virginia')

        Return
        ------
        bool
        """
        # create a file inventory
        fi = lcd.FileInventory()
        # read the csv file
        if(fi.from_file(filename) == False):
            return False
        # process the file inventory dataframe using pds class method
        df = fi.get_inventory()
        # process dataframe with pdsdocument.from_dataframe method
        return (self.from_dataframe(df, metadata))        
    
    def get_mets_file(self):
        """
        Get the METS file information.

        Return
        ------
        DataFrame
        
        """
        mets = self._pdsdocument.get_mets_file()
        return mets

    def get_image_files(self):
        """
        Get the image files information.

        Return
        ------
        DataFrame
        """
        return self._pdsdocument.get_image_files()
    
    def get_msft_img_files(self):
        """
        Get the MSFT image file information.

        Return
        ------
        DataFrame
        """
        msft_df = self._msft_fi.get_inventory()
        if (msft_df.empty == False):
            return (self._msft_fi.get_msft_img_files())
        return None
    
    def get_msft_json_files(self):
        """
        Get the MSFT JSON file information.

        Return
        ------
        DataFrame
        """
        msft_df = self._msft_fi.get_inventory()
        if (msft_df.empty == False):
            return(self._msft_fi.get_msft_json_files())
        return None
    
    def get_msft_txt_files(self):
        """
        Get the MSFT TXT files information.

        Return
        ------
        DataFrame
        """
        msft_df = self._msft_fi.get_inventory()
        if (msft_df.empty == False):
            return(self._msft_fi.get_msft_txt_files())
        return None
    
    def get_ocr_files(self):
        """
        Get the OCR files information.

        Return
        ------
        DataFrame
        """
        if (self._ocr_fi.initd()):
            return(self._ocr_fi.get_files())
        return None
    
    def get_metadata(self):
        """
        Get the metadata associated with this SAEF digital object

        Return
        ------
        dict
        """
        return self._saef_metadata
    
    def get_files(self):
        """
        Get the DataFrame containing information about all the digital object's files

        Return
        ------
        DataFrame
        """
        pds = self._pdsdocument.get_files()
        msft = self._msft_fi.get_files()
        ocr = self._ocr_fi.get_files()
        return pd.concat([pds, msft, ocr])
    
    def get_pds_relationships(self):
        """
        Get the DataFrame containing relationships between the METS and image files.

        Return
        ------
        DataFrame
        """
        return self._pds_relationships
    
    def get_msft_relationships(self):
        """
        Get the DataFrame containing relationships between the MSFT files and image files.

        Return
        ------
        DataFrame
        """
        return self._msft_relationships   

    def get_ocr_relationships(self):
        """
        Get the DataFrame containing relationships between the OCR files and image files

        Return
        ------
        DataFrame
        """
        return self._ocr_relationships
    
    def write_relationships(self, filename, relationship):
        """
        Write a named relationships DataFrame to a file

        Parameters
        ----------
        filename : str
            Path of the file to write
        relationship : str (msft | ocr | pds)

        Return
        ------
        bool
        """
        # check parameters
        # filename cannot be blank or consist of spaces ("" vs. " ")
        if not (filename and filename.strip()):
            return False
        
        # valid relationship values
        relationships = ['msft','ocr','pds']        
        # check relationship values
        if (not relationship in relationships):
            return False 
        
        # get the requested relationship dataframe
        df = None
        if (relationship == 'msft' and self._msft_relationships.empty == False):
            df = self._msft_relationships
        elif (relationship == 'pds' and self._pds_relationships.empty == False):
            df = self._pds_relationships            
        elif (relationship == 'ocr' and self._ocr_relationships.empty == False):
            df = self._ocr_relationships
        else: 
             return False
        
        # write to the relationships file
        df.to_csv(filename, sep=',', header=True,index=False)
        
        return True
    
    def initd(self):
        """
        Get instance initialization status

        Return
        ------
        bool
        """
        return self._initd

class SAEFDatasetMetadata():
    """
    Class containing Dataverse installation configuration information 
    and metadata needed to create a SAEFDataset.
    Note: This class is not usually instantiated directly by users. 
    Instead, an instance is created by the SAEFDataset::initialize method, however.

    Methods
    -------
    from_dict : dict
        Initialize a SAEFMetadata instance from a dictionary of metadata values.
        See SAEFDatasetMetadata::valid_metadata_elements for list of values.
    initd : void
        Get the instance initialization status.
    """

    def __init__(self):
        """
        Class constructor
        """

        # metadata dictionary
        self.metadata = {}
        # valid metadata elements
        self.valid_metadata_elements = ['pds_filename',
                                        'msft_filename',
                                        'ocr_filename',
                                        'object_tags',
                                        'title',
                                        'description',
                                        'author_name',
                                        'author_affiliation',
                                        'contact_name',
                                        'contact_affiliation',
                                        'contact_email',
                                        'urn',
                                        'url',
                                        'subject',
                                        'record_id',
                                        'dataverse_collection_url',
                                        'dataverse_installation_url',
                                        'dataverse_api_logfile']
        # metadata
        self.metadata = {
            'digital_object':{
                'pds_filename':None,
                'msft_filename':None,
                'ocr_filename':None,
                'object_osn':None
            },
            'dataset':{
                'title':None,
                'author':None,
                'description':None,
                'contact':None,
                'subject':None,
                'origin_of_sources':None,
                'kind_of_data':None,
                'geospatial':None,
                'customSAEF':None,
            },
            'dataverse':{
                'dataverse_collection_url':None,
                'dataverse_installation_url':None,
                'dataverse_api_logfile':None
            }
        }
        
        # initialized?
        self._initd = False

    def __parse_object_tags(self, string):
        """
        Private: Break a semi-colon delimited string of object tags into a dictionary of tag=values

        Parameter
        ---------
        string : str
            Semi-colon delimited string (e.g., Created:1863;Person/Org:Allen, Nathan)

        Return
        ------
        dict
        """
        # if the string is empty or not a string, return
        if ((not string) or (not isinstance(string,str))) :
            return {}
        # create the tag dictionary
        tag_dict = {}
        # tokenize the string
        components = string.split(';')
        for component in components:
            elements = component.split(':')
            # strip whitespaces
            tag = elements[0].strip()
            value = elements[1].strip()
            # if the tag isn't in the dictionary, add it
            if (tag not in tag_dict.keys()):
                tag_dict[tag] = []
            tag_dict[tag].append(value)
        return tag_dict    
    
    def __process_tags(self, tags):
        """
        Private: Given a dictionary of tags, process them to create well-formatted metadata elements

        Parameter
        ---------
        tags : dict

        Return
        ------
        dict
        """
        # create the metadata dictionary to return
        md = {}
        # assign easy elements first
        md['kind_of_data'] = tags.get('Physical Format')
        md['person_org'] = tags.get('Person/Org')
        md['theme'] = tags.get('Theme')
        md['genre'] = tags.get('Genre')
        md['created'] = tags.get('Created')
        
        # create the geospatial metadata element
        city = tags.get('City')
        city_elements = []
        state = tags.get('State')
        state_elements = []
        country = tags.get('Country')
        country_elements = []
        other_elements = []
        
        # process city elements
        if (city):
            for c in city:
                city_elements.append({'city':c})
        # process state elements
        if (state):
            for s in state:
                state_elements.append({'state':s})
        # process country elements
        if (country):
            for c in country:
                # TO DO: Great Britain and Wales are the only countries not on this list (ISO 3166-1)
                # https://docs.google.com/spreadsheets/d/13HP-jI_cwLDHBetn9UKTREPJ_F4iHdAvhjmlvmYdSSw/edit#gid=4
                # eventually, we should check the country against this list and append to country or other, accordingly
                if ((c == 'Great Britain') or
                    (c == 'Wales')):
                    other_elements.append({'otherGeographicCoverage':c})
                else:
                    country_elements.append({'country':c})
        # create geospatial element
        md['geospatial'] = city_elements + state_elements + country_elements + other_elements
    
        # return the dictionary
        return md
    
    def __validate_metadata(self, metadata):
        """
        Private: Validate the values in a metadata dictionary

        Raises
        ------
        TypeError
            Dictionary cannot be empty
        KeyError
            Key is missing from dictionary

        Return
        ------
        bool
        """
        if ((not isinstance(metadata, dict)) or
             (not bool(metadata))):
            raise TypeError('SAEFDatasetConfig::from_dict - metadata must be a non-empty dictionary')
            
        # validate the metadata elements
        for element in self.valid_metadata_elements:
            # if a required key is not found, raise error
            if element not in metadata.keys():
                raise KeyError('SAEFDatasetConfig::from_dict - missing key: {}'.format(element))
            # if a key=value pair is invalid (e.g. empty), raise error
            if (not metadata.get(element)):
                raise KeyError('SAEFDatasetConfig::from_dict - invalid value for key: {}'.format(element))

        # metadata is valid
        return True
    
    def from_dict(self, metadata):
        """
        Initialize a SAEFMetadata instance from a dictionary of metadata values.

        Parameter
        ---------
        metadata : dict

        Raises
        ------
        TypeError
            Input metadata is malformed

        Return
        ------
        bool
        """
        # validate the metadata
        try:
            self.__validate_metadata(metadata)
        except:
            return False
             
        # set the relationship file metadata
        self.metadata['digital_object']['pds_filename'] = metadata.get('pds_filename')
        self.metadata['digital_object']['msft_filename'] = metadata.get('msft_filename')
        self.metadata['digital_object']['ocr_filename'] = metadata.get('ocr_filename')
        self.metadata['digital_object']['object_osn'] = metadata.get('title')
        
        # collect dataverse dataset metadata values
        title = metadata.get('title')
        author = metadata.get('author_name')
        author_affiliation = metadata.get('author_affiliation')
        contact_name = metadata.get('contact_name')
        contact_affiliation = metadata.get('contact_affiliation')
        contact_email = metadata.get('contact_email')
        description = metadata.get('description')
        subject = metadata.get('subject')
        # handle lists of subjects
        subject = subject.split(',')
        urn = metadata.get('urn')
        url = metadata.get('url')
        origin_of_sources = '<a href=\"{}\">{}</a>'.format(url, urn)
        record_id = metadata.get('record_id')
        
        # strip the single quotes
        if (isinstance(record_id,str)):
            record_id = record_id.strip("'")
        else:
            record_id = ''
        
        object_tags = metadata.get('object_tags')
        if (isinstance(object_tags,str)):
            self.metadata['digital_object']['object_tags'] = object_tags.split(';')
        else:
            self.metadata['digital_object']['object_tags'] = ''

        # set default dataverse dataset metadata values
        self.metadata['dataset'] = {
            'title':title,
            'author':[{'authorName':author,'authorAffiliation':author_affiliation}],
            'description':[{'dsDescriptionValue':description}],
            'contact':[{'datasetContactName': contact_name,
                                              'datasetContactAffiliation':contact_affiliation,
                                              'datasetContactEmail':contact_email}],
            'subject':['Arts and Humanities'],
            'origin_of_sources':origin_of_sources
        }
        
        # parse object tags
        tags = self.__parse_object_tags(object_tags)
        tag_md = self.__process_tags(tags)
        
        # set additional dataset metadata values
        self.metadata['dataset']['kind_of_data'] = tag_md.get('kind_of_data')
        self.metadata['dataset']['geospatial'] = tag_md.get('geospatial')
        
        # set the custom dataset metadata values
        # TO DO: CHANGE METADATA ELEMENT NAMES UPON MOVE TO PRODUCTION
        self.metadata['dataset']['customSAEF'] = {
                'displayName':'SAEF Metadata',
                'name':'customSAEF',
                'fields':[
                    # TO DO: saefMMDID -> saefRecordID
                    {'typeName':'saefRecordID','multiple':False,'typeClass':'primitive','value':record_id}, 
                    #{'typeName':'saefMMDID','multiple':False,'typeClass':'primitive','value':record_id},               
                    # TO DO: saefCreated -> saefCreationDate
                    {'typeName':'saefCreated','multiple':True,'typeClass':'primitive','value':tag_md.get('created')},
                    #{'typeName':'saefCreationDate','multiple':True,'typeClass':'primitive','value':tag_md.get('created')},
                    {'typeName':'saefTheme','multiple':True,'typeClass':'primitive','value':tag_md.get('theme')},
                    {'typeName':'saefPersonOrgTags','multiple':True,'typeClass':'primitive',
                         'value':tag_md.get('person_org')},
                    {'typeName':'saefGenre','multiple':True,'typeClass':'primitive','value':tag_md.get('genre')}]
        }
        
        # set the dataverse metadata values
        self.metadata['dataverse']['dataverse_collection_url'] = metadata.get('dataverse_collection_url')
        self.metadata['dataverse']['dataverse_installation_url'] = metadata.get('dataverse_installation_url')
        self.metadata['dataverse']['dataverse_api_logfile'] = metadata.get('dataverse_api_logfile')
            
        # set the init flag
        self._initd = True
        return True
    
    def initd(self):
        """
        Get the instance initialization status.

        Return
        ------
        bool
        """
        return self._initd

class SAEFDataset():
    """
    Class and methods to create and manage SAEF Dataverse dataset objects. 
    SAEFDataset instances always contain a PDSDocument instance.
    They may also contain MSFTInventory and OCRInventory instances.

    Methods
    -------
    initialize : saef_digital_object, saef_project_config :
        Initialize the SAEFDataset instance.
    create : api
        Create the Dataverse dataset.
    get_dataset_metadata :  
        Get the dataset metadata.
    get_dataset_pid : void
        Get the persistent identifier for the dataset, if any.
        A valid pid is only available if the dataset has been created on the dataverse installation.
    api_upload_datafiles : api
        Upload the dataset datafiles using the Dataverse API.
    direct_upload_datafiles : api 
        Upload dataset's datafiles using a direct upload approach.
    api_upload_relationships : api
        Upload tabular relationship files, if any, using the API.
    direct_upload_relationships : api
        Upload the dataset's relationship files, if any, using direct upload method.
    upload_saef_metadata : dict
        Upload or update the dataset's SAEF custom metadata block.
    publish_dataset : api, pid
        TO DO: Publish the dataverse dataset.
    log_api_message : str, str, bool, str
        Write an API response string to a log file.
    """

    def __init__(self):
        """Class constructor"""
        #
        # members
        #
        # saef digital object instance
        self._saef_digital_object = None
        # metadata dict extracted from the saef_dataset_metadata instance
        self._metadata = None
        # doi for the dataset
        self._dataset_pid = None
        # database id for the dataset
        self._dataset_dbid = None
        # object name
        self._object_osn = None
        # api logfile
        self._api_logfile = None
        # datasets logfile
        self._datasets_logfile = None
        
        # instance is/not initialized
        self._initd = False

    def __get_lock_status(self, api):
        """
        Private: Check the dataset lock status on tabular file ingest
        Prevents failure of successive tabular file uploads via API
        See: https://github.com/IQSS/dataverse-uploader/blob/master/dataverse.py#L92-L94

        Parameter
        ---------
        api : pyDataverse API

        Return
        ------
        bool
        """
        # get dataverse server
        dataverse_installation_url = self._metadata.get('dataverse').get('dataverse_installation_url')

        # create the query string
        query_str = dataverse_installation_url + '/api/datasets/' + str(self._dataset_dbid) + '/locks/'
        
        # import requests library
        resp_ = requests.get(query_str, auth = (api.api_token, ""))
        
        # request lock status
        locks = resp_.json()['data']
        
        # handle dataset locks
        if bool(locks):
            return True
        
        # otherwise, return false
        return False     

    def initialize(self, saef_digital_object, saef_project_config):   
        """
        Initialize the SAEFDataset instance.

        Parameter
        ---------
        saef_digital_object : SAEFDigitalObject instance
        saef_project_config : SAEFProjectConfig instance
        
        Raises
        ------
        TypeError
            Input must be initialized and a SAEFDigitalObject or SAEFProjectConfig
        """ 
        # saef digital object must be valid
        if ((saef_digital_object == None) or
            (not isinstance(saef_digital_object, object)) or # 2022/12/06 check object instead of SAEFDigitalObject
            (saef_digital_object.initd() == False)):
            raise TypeError('SAEFDataset::initialize - input must be initialized and type SAEFDigitalObject')
        
        # saef project config must be valid
        # note: if isinstance fails, ensure you aren't loading saef.ipynb too many times!
        # see also: https://python-notes.curiousefficiency.org/en/latest/python_concepts/import_traps.html#the-double-import-trap
        if ((saef_project_config == None) or
            #(not isinstance(saef_project_config, SAEFProjectConfig)) or
            (saef_project_config.initd() == False)):
            raise TypeError('SAEFDataset::initialize - input must be initialized and type SAEFProjectConfig')
        
        #        
        # get metadata from the saef digital object and saef project config
        #
        
        # get the saef digital object metadata
        saef_md = saef_digital_object.get_metadata()
        object_osn = saef_md.get('object_osn')
        
        # get the config options
        options = saef_project_config.get_options()
        
        # get digital object options
        directory = options.get('digital_object').get('digital_object_relationships_directory')
        pds_file = options.get('digital_object').get('digital_object_pds_relationships')
        msft_file = options.get('digital_object').get('digital_object_msft_relationships')
        ocr_file = options.get('digital_object').get('digital_object_ocr_relationships')
        
        # set values on the saef metadata dictionary
        md = {}
        md['pds_filename'] = directory + '/' + object_osn + '_' + pds_file
        md['msft_filename'] = directory + '/' + object_osn + '_' + msft_file
        md['ocr_filename'] = directory + '/' + object_osn + '_' + ocr_file
        md['title'] = object_osn
        md['author_name'] = options.get('dataset').get('dataset_author')
        md['author_affiliation'] = options.get('dataset').get('dataset_author_affiliation')
        md['contact_name'] = options.get('dataset').get('dataset_contact_name')
        md['contact_affiliation'] = options.get('dataset').get('dataset_contact_affiliation')
        md['contact_email'] = options.get('dataset').get('dataset_contact_email')
        md['subject'] = options.get('dataset').get('dataset_subject')
        md['description'] = saef_md.get('object_title')
        md['urn'] = saef_md.get('object_delivery_urn')
        md['url'] = 'https://nrs.harvard.edu/' + md['urn']
        md['record_id'] = saef_md.get('mms_id')
        md['object_tags'] = saef_md.get('object_tags')
        md['dataverse_collection_url'] = options.get('dataverse').get('dataverse_collection_url')
        md['dataverse_installation_url'] = options.get('dataverse').get('dataverse_installation_url')
        md['dataverse_api_logfile'] = options.get('dataverse').get('dataverse_api_logfile')

        # initialize the saef dataset metadata
        saefdmd = SAEFDatasetMetadata()
        try:
            saefdmd.from_dict(md)
        except:
            raise TypeError('SAEFDataset::initialize - Failed to initialize SAEFDatasetMetadata')

        # if metadata initialization succeeded     
        self._metadata = saefdmd.metadata
        self._saef_digital_object = saef_digital_object
        self._api_logfile = options.get('dataverse').get('dataverse_api_logfile')
        self._object_osn = object_osn
               
        # ensure the logfiles can be used later
        try:
            with open(self._api_logfile, 'a') as fp:
                fp.close()
        except:
            print('failed to open logfile: {}'.format(self._api_logfile))
            return False
        
        self._initd = True
        return True
    
    def create(self, api):
        """
        Create the Dataverse dataset.

        Parameter
        ---------
        api : pyDataverse API

        Return
        ------
        bool
        
        """
        if (self._initd == False):
            return False
        
        # if the dataset has been initialized, create a pydataverse dataset model
        #
        # note: as of 2022/08/09, pydataverse does not support custom metadata,
        # therefore, i use the Dataset model to build elements for all but
        # the saef custom metadata--for ease of metadata creation
        from pyDataverse.models import Dataset
        ds = Dataset()
        # populate the dataset model with metadata values
        ds.title = self._object_osn
        ds.author = self._metadata.get('dataset').get('author')
        ds.dsDescription =  self._metadata.get('dataset').get('description')
        ds.datasetContact = self._metadata.get('dataset').get('contact')
        ds.subject = self._metadata.get('dataset').get('subject')
        ds.originOfSources = self._metadata.get('dataset').get('origin_of_sources')
        # these fields may be null
        kind_of_data = self._metadata.get('dataset').get('kind_of_data')
        if (not kind_of_data == None):
            ds.kindOfData = self._metadata.get('dataset').get('kind_of_data')
        geospatial = self._metadata.get('dataset').get('geospatial')
        if ((not geospatial == None) or
            (not geospatial == [])):
            ds.geographicCoverage = self._metadata.get('dataset').get('geospatial')
        
        # ensure that the metadata is valid
        if (ds.validate_json() == False):
            return False
        
        # 
        # prepare to create the dataset via the dataverse api
        #
        
        # import requests library
        import requests
        # get the base url
        base_url = api.base_url
        # get the api token
        api_token = api.api_token
        # dataverse collection url 
        dataverse_url = self._metadata.get('dataverse').get('dataverse_collection_url')
        # create the headers
        headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}
        # create the request url
        request_url = '{}/api/dataverses/{}/datasets'.format(base_url, dataverse_url)
        # call the requests library using the request url
        response = requests.post(request_url, headers=headers, data=ds.json())
        # get the status and message from the response
        status = int(response.status_code)
        message = response.json().get('message')
        # log the event
        msg = '{} = {}'.format(self._object_osn, message)
        self.log_api_message('SAEF::create_dataset', 'api.create_dataset', status, msg)
        
        # handle responses
        if (not ((status >= 200) and
           (status < 300))):
            print('SAEFDataset::create_dataset: Error: {} - failed to create dataset {}'.format(status, ds.json()))
            return False

        # if success, set and log the persistentid
        self._dataset_pid = response.json().get('data').get('persistentId')
        self._metadata['dataset']['doi'] = self._dataset_pid
        self._dataset_dbid = response.json().get('data').get('id')
        msg = '{} - dataset_pid={}'.format(self._object_osn, self._dataset_pid)
        self.log_api_message('SAEF::create_dataset', 'api.create_dataset', status, msg)
        
        return True
            
    def get_dataset_metadata(self):
        """
        Get the dataset metadata.

        Return
        ------
        dict
        """
        return self._metadata
    
    def api_upload_datafiles(self, api):
        """
        Upload the dataset datafiles using the Dataverse API.
        Note: Using this call is NOT recommended due to its negative
        impact on Dataverse installation performance. 
        Use direct_upload_datafiles instead.

        Parameter
        ---------
        api : pyDataverse API

        Return
        ------
        bool
        """
        if (self._initd == False):
            return False 
        
        if (self._dataset_pid == None):
            return False 
        
        # if the instance has been initialized, get the digital object
        saefdo = self._saef_digital_object
        inventory = saefdo.get_files()
        
        # import time library to support waits
        import time
        
        # iterate through the inventory
        for row in inventory.iterrows():         
            # prepare the datafile metadata
            pid = self._dataset_pid
            filepath = None
            file_format = row[1].get('file_format')
            filepath = row[1].get('file_path')
            restrict = True
            access = row[1].get('object_access')
            if (access == 'P'):
                restrict = False
            urn = row[1].get('object_delivery_urn')
            title = row[1].get('object_title')
            url = 'https://nrs.harvard.edu/' + urn
            description = 'File associated with: {} Origin of source: {}'.format(title, url)
            categories = ['Data']
            # append the user-supplied object id
            categories.append('UID:' + self._object_osn)
            # append file tags
            tags = self._metadata.get('digital_object').get('object_tags')
            for tag in tags:
                categories.append(tag)

            # create the pyDataverse datafile
            from pyDataverse.models import Datafile
            datafile = Datafile()
            # set the metadata on the datafile
            datafile.set({'pid': pid, 'filename': filepath, 'restrict':restrict,
                          'description':description, 'categories':categories})
            
            # upload the datafile via the api
            response = api.upload_datafile(pid, filepath, datafile.json(), is_pid=True)
            status = int(response.status_code)
            if (not ((status >= 200) and
               (status < 300))):
                msg = 'Upload failed: {}'.format(response.status_code)
                print('SAEFDataset::upload_datafiles: Error - failed to upload datafile {}. {}'.format(filepath, msg))
                # log the event
                message = '{} - filename: {} - {}'.format(self._object_osn, filepath, msg)
                self.log_api_message('SAEF::upload_datafiles', 'api.upload_datafile: {}'.format(filepath), status, message)
            else:
                # log the successful event
                msg = '{} - {}'.format(self._object_osn, filepath)
                self.log_api_message('SAEF::upload_datafiles', 'api.upload_datafile', status, msg)
                        
            # sleep 5 seconds to all for reindexing
            time.sleep(5)
                
        # return 
        return True 
    
    def direct_upload_datafiles(self, api):
        """
        Upload dataset's datafiles using a direct upload approach.
        This method does not require reindexing and has better performance characteristics.

        Parameter
        ---------
        api : pyDataverse API

        Return
        ------
        bool
        """
        # check initialization status
        if (self._initd == False):
            return False
        
        # dataset must exist
        if (self._dataset_pid == None):
            return False
        
        # if the instance has been initialized, get the digital object
        saefdo = self._saef_digital_object
        inventory = saefdo.get_files()
        
        # set direct upload function variables
        dataverse_url = api.base_url
        key = api.api_token
        dataset_pid = self._dataset_pid
        
        # get mets file metadata
        mets = saefdo.get_mets_file()
        index = list(mets.index)[0]
        urn = mets.at[index,'object_delivery_urn']
        restrict = True
        access = mets.at[index,'object_access']
        if (access == 'P'):
            restrict = False
        title = mets.at[index, 'object_title']
        url = 'https://nrs.harvard.edu/' + urn
        description = 'File associated with: {} Origin of source: {}'.format(title, url)
        categories = ['Data']
        # append the user-supplied object id
        categories.append('UID:' + self._object_osn)
        # append file tags
        tags = self._metadata.get('digital_object').get('object_tags')
        for tag in tags:
            categories.append(tag)
            
        # per file json_data array
        json_data = []
        
        # iterate through the inventory
        for row in inventory.iterrows():
            filepath = None
            file_format = row[1].get('file_format')
            filepath = row[1].get('file_path')
            mime_type = mimetypes.guess_type(filepath, strict=True)[0]
            components = os.path.split(filepath)
            path = components[0]
            filename = components[1]
            
            # upload the datafile
            data = {}
            data = ddu.direct_upload(dataverse_url, dataset_pid, key, filename, path, mime_type, retries=10)

            # if direct file upload failed
            if (data == None):
                msg = 'Upload failed: {}'.format('direct_upload failed')
                # log the event
                message = '{} - filename: {} - {}'.format(self._object_osn, filepath, msg)
                self.log_api_message('SAEF::direct_upload_datafiles', 'api.upload_datafile: {}'.format(filepath), 
                                     'Direct upload failed', message)
            else:
                # log the successful event
                msg = '{} - {}'.format(self._object_osn, filepath)
                self.log_api_message('SAEF::direct_upload_datafiles', 'api.direct_upload_datafiles', data, msg)
                
                # capture the file metadata for later user
                data['description'] = description
                data['categories'] = categories
                json_data.append(data)
            
        # finalize the direct upload
        status = ddu.finalize_direct_upload(dataverse_url, dataset_pid, json_data, key)
        return status
    
    def api_upload_relationships(self, api):
        """
        Upload tabular relationship files, if any, using the API.
        Note: This method has negative impact on Dataverse installation performance.
        Use direct_upload_relationships instead.

        Parameters
        ----------
        api
            pyDataverse API instance
        
        Return
        ------
        bool
        """
        # check for initialization status
        if (self._initd == False):
            return False 
        
        # ensure the dataset pid is not null
        if (self._dataset_pid == None):
            return False 
        
        # if the instance has been initialized, get the digital object
        saefdo = self._saef_digital_object
        uid = self._object_osn
        
        # dictionary of relationships data
        reldata = {}
        reldata['pds'] = {'filename':self._metadata.get('digital_object').get('pds_filename'), 'tag':'PDS'}
        reldata['msft'] = {'filename':self._metadata.get('digital_object').get('msft_filename'), 'tag':'MSFT'}
        reldata['ocr'] = {'filename':self._metadata.get('digital_object').get('ocr_filename'), 'tag':'OCR'}
        
        # dictionary of relationships files
        relationships = {}
        
        # write and record the relationships, if any
        for key in reldata.keys():
            tag = reldata.get(key).get('tag')
            filename = reldata.get(key).get('filename')
            if (saefdo.write_relationships(filename, key) == False):
                print('SAEFDataset::upload_relationships: Warning - no {} relationships file.'.format(tag))
            else:
                relationships[key] = {}
                description = 'Automatically generated {} relationship file for dataset: {}. Generated on: {}'.format(tag,
                                                                                                                      self._dataset_pid,
                                                                                                                      datetime.datetime.now())
                relationships[key]['filename'] = filename
                relationships[key]['description'] = description
                tagstr = 'SAEF:{} Relationship'.format(tag)
                relationships[key]['categories'] = ['Documentation',tagstr,'UID:' + uid]
                tags = self._metadata.get('digital_object').get('object_tags')
                for tag in tags:
                    relationships[key]['categories'].append(tag)
        
        # if there are no relationships defined, return False
        if (len(relationships.keys()) == 0):
            # nothing to upload
            return False

        # create the pyDataverse datafiles
        from pyDataverse.models import Datafile
        datafiles = []
        for key in relationships.keys():           
            datafile = Datafile()
            # set the metadata on the datafile
            filename = relationships.get(key).get('filename')
            description = relationships.get(key).get('description')
            categories = relationships.get(key).get('categories')
            datafile.set({'pid':self._dataset_pid,
                          'filename': filename,
                          'restrict':False,
                          'description':description,
                          'categories':categories})
            datafiles.append(datafile)
            
        # import time (for time.sleep())
        import time
        
        # upload the datafile via the api
        for datafile in datafiles:
            # wait for any dataset locks to clear
            while (self.__get_lock_status(api) == True):
                time.sleep(2)
            
            # get datafile metadata
            md = datafile.get()
            # get the filename (really full path of file)
            filename = md.get('filename')
            response = api.upload_datafile(self._dataset_pid, filename, datafile.json(), is_pid=True)
            status = int(response.status_code)
            if (not ((status >= 200) and
               (status < 300))):
                msg = 'Upload failed: {}'.format(response.status_code)
                print('SAEFDataset::upload_relationships: Error - failed to upload datafile {}. {}'.format(filename, msg))
                # log the event
                message = '{} - filename: {} - {}'.format(self._object_osn, filename, msg)
                self.log_api_message('SAEF::upload_relationships', 'api.upload_datafile: {}'.format(filename), status, message)                
            else:
                # log the event
                msg = '{} - {}'.format(self._object_osn, filename)
                self.log_api_message('SAEF::upload_relationships', 'api.upload_datafile', status, msg)

        # return
        return True

    def direct_upload_relationships(self, api):
        """
        Upload the dataset's relationship files, if any using direct upload method.

        Parameter
        ---------
        api : pyDataverse API

        Return
        ------
        bool
        """
        # check for initialization status
        if (self._initd == False):
            return False 
        
        # ensure the dataset pid is not null
        if (self._dataset_pid == None):
            return False 
        
        # if the instance has been initialized, get the digital object
        saefdo = self._saef_digital_object
        uid = self._object_osn
        
        # set direct upload function variables
        dataverse_url = api.base_url
        api_key = api.api_token
        dataset_pid = self._dataset_pid
        
        # dictionary of relationships data
        reldata = {}
        reldata['pds'] = {'filename':self._metadata.get('digital_object').get('pds_filename'), 'tag':'PDS'}
        reldata['msft'] = {'filename':self._metadata.get('digital_object').get('msft_filename'), 'tag':'MSFT'}
        reldata['ocr'] = {'filename':self._metadata.get('digital_object').get('ocr_filename'), 'tag':'OCR'}
        
        # dictionary of relationships files
        relationships = {}
        
        # per file json_data array
        json_data = []
        
        # write and record the relationships, if any
        for key in reldata.keys():
            tag = reldata.get(key).get('tag')
            filename = reldata.get(key).get('filename')
            if (saefdo.write_relationships(filename, key) == False):
                print('SAEFDataset::direct_upload_relationships: Warning - no {} relationships file.'.format(tag))
            else:
                relationships[key] = {}
                description = 'Automatically generated {} relationship file for dataset: {}. Generated on: {}'.format(tag,
                                                                                                                      self._dataset_pid,
                                                                                                                      datetime.datetime.now())
                relationships[key]['filename'] = filename
                relationships[key]['description'] = description
                tagstr = 'SAEF:{} Relationship'.format(tag)
                relationships[key]['categories'] = ['Documentation',tagstr,'UID:' + uid]
                tags = self._metadata.get('digital_object').get('object_tags')
                for tag in tags:
                    relationships[key]['categories'].append(tag)
        
        # if there are no relationships defined, return False
        if (len(relationships.keys()) == 0):
            # nothing to upload
            return False        
        
        # upload each relationship file and its metadata
        for key in relationships.keys():
            filepath = relationships.get(key).get('filename')
            mime_type = 'text/csv'
            components = os.path.split(filepath)
            path = components[0]
            filename = components[1]
            
            # upload the datafile
            data = {}
            data = ddu.direct_upload(dataverse_url, dataset_pid, api_key, filename, path, mime_type, retries=10)
            data['description'] = relationships[key]['description']
            data['categories'] = relationships[key]['categories']
            
            # populate the json_data array
            json_data.append(data)
            
            # manage status
            if (data == None):
                msg = 'Upload failed: {}'.format('direct_upload failed')
                print('SAEFDataset::direct_upload_relationships: Error - failed to upload datafile: {}. {}'.format(filepath, msg))
                # log the event
                message = '{} - filename: {} - {}'.format(self._object_osn, filepath, msg)
                self.log_api_message('SAEF::direct_upload_relationships', 'api.direct_upload_relationships: {}'.format(filepath), status, message)
            else:
                # log the successful event
                msg = '{} - {}'.format(self._object_osn, filepath)
                self.log_api_message('SAEF::direct_upload_relationships', 'api.direct_upload_relationships', data, msg)
            
        # finalize the direct upload
        status = ddu.finalize_direct_upload(dataverse_url, dataset_pid, json_data, api_key)
        return status

        # return
        return True        
        
    def upload_saef_metadata(self, api, metadata):
        """
        Upload or update the dataset's SAEF custom metadata block.

        Parameter
        ---------
        api : pyDataverse api
        metadata : dict
            SAEF custom metadata values.
            Also: self._metadata[dataset][customSAEF]
        
        Return
        ------
        bool
        """
       # set up the request
        import requests
        # get the base url
        base_url = api.base_url
        # get the api token
        api_token = api.api_token
        # create the headers
        headers = {'X-Dataverse-key': api_token, 'Content-Type' : 'application/json'}

        # create the request url
        request_url = '{}/api/datasets/:persistentId/editMetadata/?persistentId={}&replace=true'.format(base_url, self._dataset_pid)

        # call the requests library using the request url
        response = requests.put(request_url, headers=headers, data=metadata)
        status = int(response.status_code)
        if (not ((status >= 200) and
            (status < 300))):
            msg = 'Metadata update failed: {}'.format(response.status_code)
            print('SAEFDataset::upload_saef_metadata: Error - failed to update custom metadata {}.'.format(msg))
            # log the event
            self.log_api_message('SAEF::upload_saef_metadata', 'api.editMetadata: {}'.format(self._dataset_pid), status, msg)
            return False                
        else:
            # log the event
            msg = '{} - {}'.format(self._object_osn, self._dataset_pid)
            self.log_api_message('SAEF::upload_saef_metadata', 'api.editMetadata', status, msg)
            return True
    
    def log_api_message(self, function, api_operation, status, message):
        """
        Write an API response string to a log file.

        Parameters
        ----------
        function : str
        api_operation : str
        status : str
        message : str

        """
        import datetime
        now = datetime.datetime.now()
        with open(self._api_logfile, 'a') as fp:
            str = '{}\t{}\t{}\t{}\t{}\n'.format(now, function, api_operation, status, message)
            fp.write(str)
        fp.close()
        return

    def get_dataset_pid(self):
        """
        Get the persistent identifier for the dataset, if any.
        A valid pid is only available if the dataset has been created on the dataverse installation.

        Return
        ------
        str
        """
        return self._dataset_pid

    # end file