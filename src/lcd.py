"""
(Harvard) Library Collection as Data Classes (LCD).

Python classes supporting the management and preparation of Harvard
Library's digital collections content.
"""

from abc import ABC
from abc import ABC, abstractmethod
import os
import pandas as pd

class FileInventory():
    """
    Class that manages metadata about digital objects' files.
    File metadata is expected to be in comma-delimited format. 
    Metadata must include the fields below.

    Required File Inventory Fields
    ------------------------------
    object_osn : str
        Owner-supplied name for the digital object
    object_title : str
        Title assigned to the digital object (propagated to its
        associated files)
    object_delivery_urn : str
        URN for the digital object
    object_oasis_urn : str
        URN for the Oasis finding aid for this digital object 
        (can be empty)
    object_access : str
        File access, either `P` (public) or `R` (restricted)
    file_urn : str
        URN for the file
    file_osn : str
        Owner supplied name for file
    file_path : str
        Local path for the file. This field may be blank.
    filename : str
        Filename, including file extension.
    file_format : str
        Format for the file,for instance JP2 or JSON

    Methods
    -------
    from_dataframe : DataFrame
        Set a inventory from a DataFrame.
    from_file : str
        Set an inventory from a named file.
    get_inventory : 
        Get the current file inventory DataFrame.
    get_inventory_metadata : 
        Get the metadata for the current inventory as a list of dict.
    file_in_inventory : str
        Return True if the named file is in the inventory DataFrame, 
        False otherwise.
    get_files (field, value) : str, str
        Get a DataFrame, if any, containing the list of files that 
        match field=value.
    get_metadata : DataFrame
        Returns list of dict of inventory contents, if any.
    initd :
        Returns instance initialization status, True or False.
    """
    def __init__(self): 
        """Class constructor. Returns unitialized class instance."""

        # valid inventory fields
        self._constants = [
            # urn for the digital object
            'object_delivery_urn', 
            # owner-supplied name for the digital object 
            'object_osn', 
            # urn for the oasis finding aid associated with the digital object
            'object_oasis_urn', 
            # urn for the file
            'file_urn', 
            # format of the file, e.g., JSON, JP2
            'file_format', 
            # owner-supplied name for the file
            'file_osn', 
            # title assigned to the digital object (and propagated to its associated files)
            'object_title', 
            # file access, either P (public) or R (restricted)
            'object_access', 
            # name of the file, including its extension
            'filename', 
            # full path to the file
            'file_path'] 
        
        # the file inventory dataframe
        self._inventory_df = pd.DataFrame()
        
        # instance initialized?
        self._initd = False
    
    def __validate(self, inventory_df):
        """
        Private: Validates a DataFrame inventory.

        Parameters
        ----------
        inventory_df: DataFrame
            Inventory of file metadata

        Raises
        ------
        TypeError
            If input is not a DataFrame
        ValueError
            If input is missing a required file metadata field

        Returns
        -------
        bool
        """
        # input must be DataFrame
        if not isinstance(inventory_df, pd.DataFrame):
            raise TypeError('FileInventory::__validate - input must be DataFrame')
        
        # get the field names
        for field in self._constants:
            if field not in self._inventory_df.columns:
                raise ValueError('FileInventory::__validate: Error - missing field: {}'.format(field))
        
        return True
    
    def from_dataframe(self, inventory_df):
        """
        Set or initialize an inventory using a DataFame.

        Parameter
        ----------
        inventory_df: DataFrame
            Inventory of file metadata

        Catches
        ------
        TypeError
            Input is not a DataFrame
        ValueError
            Input is missing a required field

        Return
        ------
        bool
        """
        # set the inventory dataframe member variable
        self._inventory_df = inventory_df
        # try to validate the inventory dataframe
        try:
            # validate the inventory_df
            status = self.__validate(self._inventory_df)
        # catch exceptions
        except TypeError:
            # input is not a dataframe
            return False
        except ValueError:
            # missing field in input dataframe
            return False     
        else:
            # set init status
            self._initd = True
            return True
    
    def from_file(self, filename):
        """
        Initialize an inventory from a file

        Parameter
        ---------
        filename : str
            Path of file to read
        
        Returns
        -------
        bool
        """
        # filename must not be empty string
        if (not filename):
            return False
        
        # read the inventory file
        inventory_df = pd.read_csv(filename, sep=',',header=0)

        # cast all fields to type object
        # note: this avoids having blank fields default to NaN/float64
        self._inventory_df = inventory_df.astype('object')
        
        try:
            # validate the inventory_df
            status = self.__validate(self._inventory_df)
        # catch exceptions
        except TypeError:
            # input is not a dataframe
            return False
        except ValueError:
            # missing field in dataframe
            return False      
        else:
            self._initd = True
            return True

    def get_inventory(self):
        """
        Return the instance's inventory DataFrame.

        Return
        ------
        DataFrame
        """
        return self._inventory_df
    
    def file_in_inventory(self, filename):
        """
        Look for a filename in the current FileInventory instance.
        
        Parameter
        ---------
        filename : str
            Name of the file (with extension) to locate in the current inventory.
        
        Return
        ------
        Tuple (bool, DataFrame)
        """

        # is the inventory empty?
        if (self._inventory_df.empty == True):
            return False, None
        
        # look for the filename in the full inventory
        file = self._inventory_df.loc[self._inventory_df['filename']== filename]
        if (file.empty == False):
            # if the return dataframe contains files
            # return True and the dataframe
            return True, file
        # filename was not found
        return False, None
    
    def get_files(self, field, value):
        """
        Return all inventory files with a field matching value.

        Parameters
        ----------
        field : str
            Name of the field 
        value : str
            Value of the field name
        
        Return
        ------
        DataFrame
        """

        # is the inventory empty?
        if (self._inventory_df.empty == True):
            return pd.DataFrame()
        
        # is the field valid?
        if field not in self._constants:
            raise NameError('FileInventory::get_files - invalid field name')
            
        # query the inventory for matching files
        files = self._inventory_df[self._inventory_df[field] == value]
        return files

    def get_owner_supplied_names (self):
        """
        Get the unique owner-supplied names in the inventory.

        Return
        ------
        list
        """
        # instance must be initialized
        if (self._initd == False):
            return []
        
        # if the dataframe is empty, return empty list
        if (self._inventory_df.empty == True):
            return []
    
        # get the unique ids
        return self._inventory_df['object_osn'].unique()

    def initd(self):
        """
        Get instance initialization status
        
        Return
        ------
        bool
        """
        return self._initd

class OCRInventory (FileInventory):
    """
    Subclass of FileInventory that manages inventories of metadata about OCR text files.
    OCR files are produced by Harvard Library Imaging Services. 
    Files are of the format: Plain text.

    Methods
    -------
    from_dataframe : DataFrame
        Initialize an OCR inventory from a DataFrame.
    from_file : str
        Initialize an inventory from a file.
    get_files : void
        Get a DataFrame of OCR files in the current inventory.
    initd : void
        Get initialization status of current inventory.
    """
    # method: constructor: __init__
    def __init__(self):
        """Class constructor"""

        FileInventory.__init__(self)

        # valid ocr formats
        self._ocr_format = {'ocr_txt':'Plain text'}
        
        # init status
        self._initd = False
        
    def __validate(self, dataframe):
        """
        Private: Validate OCR inventory
        
        Parameter
        ---------
        dataframe : DataFrame

        Raises
        ------
        TypeError
            Input is not a DataFrame.
        ValueError
            Input DataFrame cannot be empty.
            DataFrame must contain only one owner-supplied name.

        Return
        ------
        DataFrame
            Empty DataFrame on failure.
        """
        # input must be DataFrame
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('OCRInventory::__validate - input must be of type DataFrame')        
        
        # dataframe cannot be empty
        if (dataframe.empty == True):
            raise ValueError('OCRInventory::__validate - DataFrame cannot be empty')    
        
        # there can only be one owner-supplied name in the DataFrame
        unique_osn = dataframe['object_osn'].unique()
        if (len(unique_osn) > 1):
            raise ValueError('OCRInventory::__validate: DataFrame must contain no more than one owner-supplied name')  
            
        # inspect the contents of the dataframe
        fi = FileInventory()
        status = fi.from_dataframe(dataframe)
        if (status == False):
            # return an empty dataframe
            return pd.DataFrame()
        
        # get the OCR files from the inventory
        ocr_txt = fi.get_files('file_format', self._ocr_format.get('ocr_txt'))
        ocr_txt_count = len(ocr_txt)
        # if there are no ocr files 
        if (ocr_txt_count == 0):
            # return an empty dataframe
            return pd.DataFrame()
        
        # all error conditions have been passed, return the inventory
        return ocr_txt
        
    def from_dataframe(self, inventory_df):
        """
        Initialize an OCR inventory from a DataFrame.

        Parameter
        ---------
        inventory_df : DataFrame

        Raises
        ------
        TypeError
            Input must be a DataFrame.
        ValueError
            DataFrame must contain required fields.

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
            # input must be a dataframe
            return False
        except ValueError:
            # invalid value in inventory
            return False            
        else:
            self._initd = True
            # return True on success
            return True

    # method: from_file
    # description: initialie an inventory from a file
    # return: True if success, False if fail. Catches exceptions raised from __validate
    def from_file(self, filename):
        """
        Initialize an inventory from a file.

        Parameter
        ---------
        filename : str
        
        Return
        -------
        bool
        
        """
        # filename must not be empty string
        if (not filename):     
            return False
                # read the inventory file

        # read the file
        inventory_df = pd.read_csv(filename, sep=',',header=0)

        # cast all fields to type object
        # note: this avoids having blank fields default to NaN/float64
        self._inventory_df = inventory_df.astype('object')
        
        try:
            # validate the inventory_df
            status = self.__validate(self._inventory_df)
        # catch exceptions
        except TypeError:
            # input is not a dataframe
            return False
        except ValueError:
            # missing field in dataframe
            return False      
        else:
            self._initd = True
            return True     
        
    def get_files(self):
        """
        Get a DataFrame of OCR files in the current inventory.
        
        Return
        ------
        DataFrame
        """
        return self._inventory_df
    
    def initd(self):
        """Get initialization status of current inventory.
        
        Return
        ------
        bool
        """
        return self._initd    

class DigitalObject(ABC):
    """
    Abstract base class for all digital objects. Defines interface for subclasses
    
    Methods
    -------
    from_dataframe : DataFrame
        Initialize instance members from a DataFrame.
    from_file : str
        Initialize the instance variables from a .csv file.
    get_metadata : void
        Get the metadata (about the DigitalObject) from the instance.
    get_files : void
        Get the DataFrame of metadata for all files associated with the instance.
    get_file_formats : void
        Get the list of valid file formats.
    initd : void
        Get instance initialization status.
    """

    # define abstract methods
    @abstractmethod

    def from_dataframe(self, dataframe):
        """Initialize instance members from a DataFrame.

        Return
        ------
        bool
        """
        pass
    
    def from_file(self, filename):
        """
        Initialize the instance variables from a .csv file.
        
        Return
        ------
        bool
        """
        pass
    
    def get_metadata(self):
        """
        Get the metadata (about the DigitalObject) from the instance.
        
        Return
        ------
        List of dict
        """
        pass
        
    def get_files(self):
        """
        Get the DataFrame of metadata for all files associated with the instance.
        
        Return
        ------
        DataFrame
        """
        pass
    
    def get_file_formats(self):
        """
        Get the list of valid file formats. 
        
        Return
        ------
        List
        """
        pass
    
    def initd(self):
        """
        Get instance initialization status.
        
        Return
        ------
        bool
        """
        pass

class PDSDocument (DigitalObject):
    """
    Class for a digital object of type PDS Document.
    A PDSDocument corresponds to a page-turned object and contains
    a single METS file and associated image files.

    Methods
    -------
    from_dataframe : DataFrame
        Initialize the PDS object with information stored in a DataFrame.
    from_file : str
        Initialize a PDSDocument from a (*.csv) file.
    get_metadata : void
        Get the PDSDocument's metadata.
    get_files : void
        Get a DataFrame of information about all the PDSDocument's files.
    get_mets_file : void
        Get the DataFrame containing the PDSDocument's METS file.
    get_image_files : void 
        Get a DataFrame of image files.
    get_file_formats : void
        Get the list of valid file formats.
    initd : void
        Get instance initialization status.
    """
    # method: constructor: __init__
    def __init__(self):
        """Class constructor"""

        # dictionary of metadata about the digital object
        self._metadata = {}
        # DataFrame containing metadata for each file in the digital object
        self._files_df = None
        # default file formats for PDSDocument objects
        # note: for future projects, this list may need to be updated to reflect additional image file formats
        self._file_formats = ['Extensible Markup Language','JPEG 2000 JP2','JPEG']
        # initialized?
        self._initd = False

    def __validate(self, dataframe):
        """
        Private: Validate that files in DataFrame match PDS Document expectations.
        
        Parameter
        ---------
        dataframe : DataFrame

        Raises
        ------
        TypeError
            Input is not a DataFrame.
        ValueError
            Input DataFrame cannot be empty.
            DataFrame must contain one owner-supplied name at most.
            DataFrame must contain one METS file at most.
            DataFrame must contain at least one image file.

        Return
        ------
        bool
        """

        # input must be DataFrame
        if not isinstance(dataframe, pd.DataFrame):
            # input is not a dataframe
            print('PDSDocument::__validate - input must be of type DataFrame')
            raise TypeError('PDSDocument::__validate - input must be of type DataFrame')        
        
        # dataframe cannot be empty
        if (dataframe.empty == True):
            # input cannot be empty
            print('PDSDocument::__validate - empty DataFrame')
            raise ValueError('PDSDocument::__validate - DataFrame cannot be empty')       
        
        # inspect the contents of the dataframe
        fi = FileInventory()
        status = fi.from_dataframe(dataframe)
        
        # there can only be one owner-supplied name in the DataFrame
        unique_osn = dataframe['object_osn'].unique()
        if (len(unique_osn) > 1):
            raise ValueError('PDSDocument::__validate: DataFrame must contain no more than one owner-supplied name')  
        
        # get the mets file
        mets = fi.get_files('file_format', 'Extensible Markup Language')
        
        # if there is more than one mets file in the dataframe, error
        if (len(mets) > 1): 
            raise ValueError('PDSDocument::__validate: Error - too many METS files in DataFrame') 
            
        # get the image files
        jp2 = fi.get_files('file_format', 'JPEG 2000 JP2')
        jpeg = fi.get_files('file_format', 'JPEG')
        images = pd.concat([jp2, jpeg])
        # if there are no image files of either format
        if (images.empty == True):
            print('no images')
            raise ValueError('PDSDocument::__validate: Error - DataFrame must contain at least one image file');
        
        # all error conditions have been passed
        return True
    
    # method: from_dataframe
    # initialize the PDS Document using a dataframe
    # return: True or False
    def from_dataframe(self, dataframe):
        """
        Initialize the PDS object with information stored in a DataFrame.
        
        Parameter
        ---------
        dataframe : DataFrame

        Return
        ------
        bool

        """
        # try to validate the dataframe
        try:
            self.__validate(dataframe)
        except:
            return False
        else:
            # otherwise, extract metadata from the dataframe
            fi = FileInventory()
            # initialize the FileInventory
            fi.from_dataframe(dataframe)
            mets = fi.get_files('file_format', 'Extensible Markup Language')
            jp2 = fi.get_files('file_format', 'JPEG 2000 JP2')
            jpeg = fi.get_files('file_format', 'JPEG')
            images = pd.concat([jp2,jpeg])
            # set the internal inventory
            self._files_df = pd.concat([mets, images])
            
            # get the (integer) index of the mets file in the dataframe
            # for details, see: https://pandas.pydata.org/docs/reference/api/pandas.Index.html
            index = list(mets.index)[0]
            # populate the pdsdocument object-level metadata
            self._metadata = {
                                'object_osn':mets.at[index,'object_osn'],
                                'object_title':mets.at[index,'object_title'],
                                'object_delivery_urn':mets.at[index,'object_delivery_urn'],
                                'object_oasis_urn':mets.at[index,'object_oasis_urn'],
                                'object_access':mets.at[index,'object_access']
            }
        self._initd = True
        return True
    
    # method: from_file
    # description: initialize pdsdocument from a .csv file
    # return True or False
    def from_file(self, filename):
        """
        Initialize a PDSDocument from a (*.csv) file.
        
        Parameter
        ---------
        filename : str

        Return
        ------
        bool
        """

        # create a file inventory
        fi = FileInventory()
        # read the csv file
        status = fi.from_file(filename)
        # process the file inventory dataframe using pds class method
        df = fi.get_inventory()
        # process dataframe with pdsdocument.from_dataframe method
        status = self.from_dataframe(df)
        return (status)

    def get_metadata(self):
        """
        Get the PDSDocument's metadata.
        
        Return
        ------
        dict
        """
        return self._metadata
    
    def get_mets_file(self):
        """
        Get the DataFrame containing the PDSDocument's METS file.
        
        Return
        ------
        DataFrame
        """
        # note: no need to handle exception, 'file_format' is valid field
        return self._files_df[self._files_df['file_format'] == 'Extensible Markup Language']      
    
    def get_image_files(self):
        """
        Get a DataFrame of image files.
        
        Return
        ------
        DataFrame
        """
        # note: no need to handle exception, 'file_format' is valid field
        jp2 = self._files_df[self._files_df['file_format'] == 'JPEG 2000 JP2']
        jpeg = self._files_df[self._files_df['file_format'] == 'JPEG']
        # concatenate the dataframes
        images = pd.concat([jp2, jpeg])
        return images

    def get_files(self):
        """
        Get a DataFrame of information about all the PDSDocument's files.
        
        Return
        ------
        DataFrame
        
        """
        return self._files_df

    def get_file_formats(self):
        """
        Get the list of valid file formats.
        
        Return
        ------
        List
        """
        return self._file_formats
    
    def initd(self):
        """
        Get initialization status for the instance.

        Return
        ------
        bool
        """
        return self._initd

# end file