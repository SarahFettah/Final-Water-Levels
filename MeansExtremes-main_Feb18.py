'''
Description:
One time script for daily and monthly means and extremes.
Using https://tides.gc.ca/en/daily-means-monthly-means-and-extremes-tides to format
Some info on site is outdated so going off of a 2022 file.

File Version:
Version 1.2

Authors:
Evan James (evan.james@dfo-mpo.gc.ca)
Khaleel Arfeen (khaleel.arfeen@dfo-mpo.gc.ca)
Chloe Tsao (chloe.tsao@dfo-mpo.gc.ca)
'''

from decimal import Decimal, ROUND_HALF_EVEN
import requests
import calendar
import logging
import time
from datetime import datetime 

header_dict = {
    '10050': '      THUNDER BAY, ONTARIO             18320024825 8913      EST1',
    '10220': '      ROSSPORT, ONTARIO                18320024850 8731      EST1',
    '10750': '      MICHIPICOTEN, ONTARIO            18320024758 8454      EST1',
    '10920': '      GROS CAP, ONTARIO                18320024850 8731      EST1',
    '10980': '      SAULT STE MARIE, ONTARIO         18300024631 8422      EST1',
    '11010': '      SAULT STE MARIE, ONTARIO         17638024631 8421      EST1',
    '11070': '      THESSALON, ONTARIO               17600024615 8333      EST1',
    '11195': '      LITTLE CURRENT, ONTARIO          17600024559 8156      EST1',
    '11375': '      PARRY SOUND, ONTARIO             17600024520 8002      EST1',
    '11445': '      MIDLAND, ONTARIO                 17600024445 7953      EST1',
    '11500': '      COLLINGWOOD, ONTARIO             17600024431 8013      EST1',
    '11690': '      TOBERMORY, ONTARIO               17600024515 8140      EST1',
    '11860': '      GODERICH, ONTARIO                17600024345 8144      EST1',
    '11940': '      POINT EDWARD, ONTARIO            17565024259 8225      EST1',
    '11950': '      PORT LAMBTON, ONTARIO            17465024239 8230      EST1',
    '11965': '      BELLE RIVER, ONTARIO             17440024218 8243      EST1',
    '11995': '      AMHERSTBURG, ONTARIO             17387024209 8307      EST1',
    '12005': '      BAR POINT, ONTARIO               17350024204 8307      EST1',
    '12065': '      KINGSVILLE, ONTARIO              17350024202 8244      EST1',
    '12250': '      ERIEAU, ONTARIO                  17350024216 8155      EST1',
    '12400': '      PORT STANLEY, ONTARIO            17350024239 8113      EST1',
    '12710': '      PORT DOVER, ONTARIO              17350024247 8012      EST1',
    '12865': '      PORT COLBORNE, ONTARIO           17350024252 7915      EST1',
    '13030': '      PORT WELLER, ONTARIO              7420024314 7913      EST1',
    '13150': '      BURLINGTON, ONTARIO               7420024318 7948      EST1',
    '13320': '      TORONTO, ONTARIO                  7420024338 7923      EST1',
    '13590': '      COBOURG, ONTARIO                  7420024357 7810      EST1',
    '13988': '      KINGSTON, ONTARIO                 7420024413 7631      EST1',
    '14400': '      BROCKVILLE, ONTARIO               7395024435 7541      EST1',
    '14600': '      UPPER IROQUOIS, ONTARIO           7324024449 7519      EST1',
    '14602': '      LOWER IROQUOIS, ONTARIO           7318024450 7519      EST1',
    '14660': '      MORRISBURG, ONTARIO               7286424454 7512      EST1',
    '14870': '      CORNWALL, ONTARIO                 4640024501 7443      EST1',
    '14940': '      SUMMERSTOWN, ONTARIO              4624024504 7433      EST1',
    '15520': '      Montreal Jetee #1                  556024530 7333      EST1',
    '15540': '      Montreal rue Frontenac             534824532 7333      EST1',
    '15660': '      Varennes                           483624541 7327      EST1',
    '15780': '      Contrecoeur IOC                    436024550 7317      EST1',
    '15930': '      Sorel                              377524603 7307      EST1',
    '15975': '      Lac Saint-Pierre                   339024612 7254      EST1',
    '03360': '      Trois-Rivieres                     292624620 7232      EST1',
    '03365': '      Port-Saint-Francois                298724616 7237      EST1' 
}

timeout = 2 # seconds # set to 2s if off DFO network (public)
# Year of data collected
year = 2025
# List of days in each month for Year
months = [i for i in range(1, 13)]
days_in_month = [calendar.monthrange(year, month)[1] for month in range(1, 13)]


class Daily_Means_File:
    def __init__(self) -> None:
        self.daily_means_for_month = []
        self.station_url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations'
        self.station_code = ""

        self.api_call_counter = 0
        print(self.api_call_counter)

        self.log_setup()
        self.make_file()


    #------------------------------------------------------------------------------------------------------------------------------------------

    def log_setup(self):
        '''
        Initializes the logger for use in developer troubleshooting
        I: None
        O: .log file created in the working directory
        '''

        log_file_name = 'IGLD_Regulation_DMF.log'

        logging.basicConfig(
            level=logging.INFO,
            format='ts="%(asctime)s" level="%(levelname)s" message="%(message)s"',
            datefmt='%Y-%m-%dT%H:%M:%S%z',
            handlers=[
                logging.FileHandler(log_file_name),
                logging.StreamHandler()])

        logging.Formatter.converter = time.gmtime

        logging.info('IGLD Regulation DMF Logger Initialized Successfully')
    #------------------------------------------------------------------------------------------------------------------------------------------

    def make_file(self) -> None:
        '''
        Writes information into the file correctly.
        '''
        #Create a file in the directory to write to
        file_name = f'CORNWALL_DMF_{str(year)}.dat'
        with open(file_name, 'w') as f:
            
            #Loop through every station
            for station_code in header_dict:
                
                #For every station, go through every month
                for month in months:
                    #Header
                    f.write(f"- {station_code} {year}{month:02d}{header_dict[str(station_code)]}\n")
                    
                    if self.api_call_counter == 35:
                        time.sleep(60) #increase time if api 429 persist
                        self.api_call_counter = 0

                    station_id = self.get_station_id(station_code) # station id is a hex identifier used by the API
                    dailyMeans = self.get_daily_means_for_month(station_id, month, days_in_month[month-1]+1)
                    #import pdb; pdb.set_trace()
                    self.api_call_counter += 2
                    print(f'{datetime.now()} {self.api_call_counter}')

                    # Split dailyMeans into 5-character chunks (one per day)
                    means_per_day = [dailyMeans[i:i+5] for i in range(0, len(dailyMeans), 5)]

                    # Get the number of days in this month
                    num_days = days_in_month[month-1]

                    # 1st to 10th
                    means_1_10 = ''.join(means_per_day[0:10])
                    f.write(f"5 {station_code} {year}{month:02d}0110      {means_1_10}\n")

                    # 11th to 20th
                    if num_days >= 11:
                        means_11_20 = ''.join(means_per_day[10:20])
                        f.write(f"5 {station_code} {year}{month:02d}1120      {means_11_20}\n")

                    # 21st to end of month
                    if num_days > 20:
                        means_21_end = ''.join(means_per_day[20:num_days])
                        f.write(f"5 {station_code} {year}{month:02d}21{days_in_month[month-1]}{self.get_monthly_mean()}{means_21_end}\n")

                    f.write(f"6 {station_code} {year}{month:02d}     9999999999999999999999 999 99\n")
                    time.sleep(timeout)

        return
                        
    #------------------------------------------------------------------------------------------------------------------------------------------
    def get_station_id(self, station_code):
        """
        Get internal station id given the station code  
        return:
            Station ID i.e. code='07120' returns id=5cebf1df3d0f4a073c4bbd1e
        """    

        self.station_code = station_code # save code to memory
        params = {'code':station_code}

        try:
            # get station ID from endpoint
            response = requests.get(url=self.station_url, params=params)
            
            if response.status_code == 200:
                #load data as json
                data = response.json()
                if not data:
                    print(f"Station does not appear to exist for: {station_code}")
                    logging.error(f"Station does not appear to exist for: {station_code}")
                    return
                station_id = data[0].get('id')
                station_code = data[0].get('code')
                print(station_code)
                return station_id
            
            else: 
                print(f"Bad response getting station code, error code: {response.status_code}")
                logging.error(f"Bad response getting station code, error code: {response.status_code}")
                return

        # problem, no data able to be gathered
        except Exception as e:
            print(f"Unexpected error occurred during 'get_station_id': {e}")

    #------------------------------------------------------------------------------------------------------------------------------------------
    def get_daily_means_for_month(self, stationId: object, month: int, days: list) -> str:
        '''
        Gets the daily mean for the set of parameters passed in.
        '''
        final_string_of_month = ""
        data = []
        api_error = False

        first_day = 1
        last_day = days-1

        #Params for calculate-daily-means-igld85 
        params = {
                'stationId' : stationId, 
                'from' : str(year) + str(f"-{month:02d}") + str(f"-{first_day:02d}"),
                'to' :  str(year) + str(f"-{month:02d}") + str(f"-{last_day:02d}")
                }
        
        bad_response_tries = 0
        max_tries = 5
        
        while bad_response_tries < max_tries:
            try:
                #Get data from endpoint
                url = f'{self.station_url}/{params["stationId"]}/stats/calculate-daily-means-igld85'
                response = requests.get(url=url, params=params)

                if response.status_code == 200:
                    #load data as json
                    api_error = False
                    data = response.json()
                    print(data)
                    break

                elif response.status_code == 404:
                    api_error = True
                    print(f"Station data not found for {self.station_code} during {year}/{month:02d}")
                    logging.error(f"Station data not found for {self.station_code} during {year}/{month:02d}")
                    #final_string_of_month += "99999" * (days-1)
                    break

                else: 
                    api_error = True
                    print(f"Bad response getting daily mean, error code: {response.status_code}")
                    print(f"    {self.station_code} Bad data for time period {year}/{month:02d}")
                    logging.error(f"Bad response getting daily mean, error code: {response.status_code}:")
                    logging.error(f"    {self.station_code} Bad data for time period {year}/{month:02d}")
                    bad_response_tries += 1
                    
                    if bad_response_tries < max_tries:
                        time.sleep(5)
                    elif bad_response_tries == max_tries:
                        #final_string_of_month += "99999" * (days-1)
                        break

            #Problem, no data able to be gathered
            except Exception as e:
                api_error = True
                print(f"Error: {e}")
                #Enter data as 9s per instructions
                #final_string_of_month += "99999" * (days-1)

        if not data:
            # data returned is an empty list
            if api_error == False:
                print(f"No data returned from API for time period {year}/{month:02d} for station {self.station_code}")
                logging.error(f"No data returned from API for time period {year}/{month:02d} for station {self.station_code}")
            final_string_of_month += "99999" * (days-1)

        else:
            last_data = data[-1]['date']
            #Check if the last data is from the last day of the month
            last_day_with_data = str(last_data)[-2:]
            if int(last_day_with_data) < days-1:
                final_string_of_month = self.count_data_by_days(data, month, int(last_day_with_data)+1)
                final_string_of_month += "99999" * (days - int(last_day_with_data) - 1) #fill in the rest of the month with 9s
            elif int(last_day_with_data) == days-1: #if yes, proceed as normal
                final_string_of_month = self.count_data_by_days(data, month, days)

        return final_string_of_month

    #------------------------------------------------------------------------------------------------------------------------------------------
    def count_data_by_days(self, data, month, days):
        day_count = 1
        final_string = ""
        while day_count < days:    # will loop until last day of month e.g. 31st for jan as days = 32
            for line in data:
                date = str(line['date'])[-2:]
                try: 
                    if int(date) == day_count:  #if the date of the current line matches the day count
                        # convert to proper decimals (ie. 74.615 recorded in python as 74.6149999 and causing issues)
                        daily_mean = Decimal(str(line['dailyMean_IGLD85']))
                        #Log daily mean for the monthly mean calculation
                        self.daily_means_for_month.append(line['dailyMean_IGLD85'])
                        #Ensure daily mean is 5 chars long
                        
                        dm = daily_mean.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN)
                        dm = "{:.2f}".format(dm).replace('.', '')
                        while len(dm) < 5:
                            dm = ' ' + dm
                        #Concat to string that will be put in the file    
                        final_string += dm
                        day_count += 1
                    elif int(date) > day_count: #if the dates don't match (usually day_count being smaller)
                        while int(date) > day_count: #repeats until the date matches the day count, then break out)
                            print(f"Missing data for {self.station_code} on {year}/{month:02d}/{day_count:02d}")
                            logging.error(f"Missing data for {self.station_code} on {year}/{month:02d}/{day_count:02d}")
                            final_string += "99999"
                            day_count += 1
                            if int(date) == day_count: #obtains the first instance where date matches day count, adds it to daily_means_for_month, break out of loop
                                # convert to proper decimals (ie. 74.615 recorded in python as 74.6149999 and causing issues)
                                daily_mean = Decimal(str(line['dailyMean_IGLD85']))
                                #Log daily mean for the monthly mean calculation
                                self.daily_means_for_month.append(line['dailyMean_IGLD85'])
                                #Ensure daily mean is 5 chars long
                                
                                dm = daily_mean.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN)
                                dm = "{:.2f}".format(dm).replace('.', '')
                                while len(dm) < 5:
                                    dm = ' ' + dm
                                #Concat to string that will be put in the file    
                                final_string += dm
                                day_count += 1
                            continue

                except Exception as e:
                    print(f"Error: {e}. Data not included in monthly mean calculation")
                    logging.error(f"Error: {e}. Data not included in monthly mean calculation")
                    final_string += "99999"
                    continue 

        return final_string
    
    #------------------------------------------------------------------------------------------------------------------------------------------
    def get_monthly_mean(self) -> float:
        '''
        Calculates and returns the monthly mean based on daily means
        '''

        monthly_mean = 0

        #Monthly mean may only be calculated if there are 20 or more daily means
        if len(self.daily_means_for_month) < 20:
            print("Not enough daily means to calculate monthly mean")
            return 999999
        
        else:
            
            #Loop through the daily means
            for mean in self.daily_means_for_month:
                #Round to 2 decimal places
                mean = Decimal(str(mean))
                dm = mean.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN)
                dm = str(dm).replace(".","")
                #Add to total
                monthly_mean += mean
            
            #Store monthly mean in a var
            r = "{:.3f}".format(monthly_mean/len(self.daily_means_for_month)).replace('.', '')
            while len(r) < 6:
                r = ' ' + r
            #Reset daily means list for next month
            self.daily_means_for_month = []

            return r
    #------------------------------------------------------------------------------------------------------------------------------------------
#                    daily_mean = Decimal(str(line['dailyMean_IGLD85']))
#                        #Log daily mean for the monthly mean calculation
#                        self.daily_means_for_month.append(line['dailyMean_IGLD85'])
#                        #Ensure daily mean is 5 chars long
                        
#                        dm = daily_mean.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN)
#                        dm = "{:.2f}".format(dm).replace('.', '')

#----------------------------------------------------------------------
if __name__=='__main__':
    instance = Daily_Means_File()

#----------------------------------------------------------------------