# Import libraries
import pandas as pd
from sodapy import Socrata

def API_to_dataframe(website, identifier, lim):
    """
    Ths function takes 3 paramters: website, identifier, and lim. The website is the
    montgomery county data website (str). The identifier is unique to the API
    of the crime data (str). The lim is the limit number of results we want returned
    by the api (int).

    return: pandas dataframe, will be uncleaned.
    """
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata(website, None)

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get(identifier, limit=lim)

    # Convert to pandas DataFrame
    dataframe = pd.DataFrame.from_records(results)

    # Return
    return dataframe

def clean_dataframe(unclean_dataframe):
    """
    This function takes the uncleaned pandas dataframe of montgomery county crime
    and cleans it.

    return: cleaned dataframe for analysis
    """

    # List of columns to interested in (in theory this is removing unwanted columns)
    # case_number: Police Report Number
    # date: Date & Time an officer was dispatched
    # victims: Number of victims of crime
    # crimename1: Name of the crime
    # district: City that crime was located in
    # location: Address of Crime
    # city, state, zip_code
    # police_district_number
    # latitude, longitude
    extract_these_col = ['case_number', 'date', 'victims', 'crimename2',
                        'district', 'location', 'city', 'state', 'zip_code',
                        'police_district_number', 'latitude', 'longitude']
    # Create a dataframe with the wanted columns extracted
    unclean_dataframe = unclean_dataframe[extract_these_col]
    # Good practice that after extraction, to reset to the index
    unclean_dataframe = unclean_dataframe.reset_index(drop=True)
    # Remove that funny T and .000 from the date column
    unclean_dataframe.date = unclean_dataframe.date.str.replace('T', ' ')
    unclean_dataframe.date = unclean_dataframe.date.str.replace('.000', '')
    # Covert the longitude and latitude columns to numeric
    unclean_dataframe.longitude = pd.to_numeric(unclean_dataframe.longitude)
    unclean_dataframe.latitude = pd.to_numeric(unclean_dataframe.latitude)
    # Convert the date column to datetime
    unclean_dataframe.date = pd.to_datetime(unclean_dataframe.date)
    # Convert victims to an number
    unclean_dataframe.victims = pd.to_numeric(unclean_dataframe.victims)
    # Clean the addresses (column == location), to make them gramatically correct
    unclean_dataframe.location = unclean_dataframe.location.str.title()
    # Clean the district, to make them gramatically correct
    unclean_dataframe.district = unclean_dataframe.district.str.title()
    # Clean the city name to make it gramatically correct
    unclean_dataframe.city = unclean_dataframe.city.str.title()
    # Clean the crime2 to make it grammatically correct
    unclean_dataframe.crimename2 = unclean_dataframe.crimename2.str.title()

    # As a data admin, you may want to go look at the rows with NaN or NaT values,
    # you might not want them for the cleaned data but it's good to keep them somewhere later
    columns = unclean_dataframe.columns
    null_dataframe = unclean_dataframe[unclean_dataframe.isnull().any(axis=1)][columns]

    # For cleaned data, we only want columns without null value
    no_null_dataframe = pd.concat([unclean_dataframe, null_dataframe, null_dataframe]).drop_duplicates(keep=False)

    # Reset the index column
    clean_dataframe = no_null_dataframe.reset_index(drop=True)

    # Return Dataframe
    return clean_dataframe

class MontgomeryCountyData:
    # Methods
    # Initialize the variable [built-in]
    def __init__(self):
        self.web = 'data.montgomerycountymd.gov'
        self.identifier = 'yc8a-5df8'
        self.limit = 2000
        print('You are now accessing authorized data...')
        print('> Accessing...%s...' % (self.web))
        print('> Idenifying...%s...' % (self.identifier))
        self.unclean_dataframe = API_to_dataframe(self.web, self.identifier, self.limit)

    def clean_data(self):
        self.clean_dataframe = clean_dataframe(self.unclean_dataframe)


if __name__ == '___main___':
    # Create an object corresponding to the data cleaning process
    d1 = MontgomeryCountyData()

    # Clean the data
    d1.clean_data()

    # Show the cleaned dataframe using the clean_data method
    d1.clean_dataframe.head()
    print('Hello')
