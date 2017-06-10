import requests

API_VERSION = '0'
API_URL = 'https://api.airtable.com/v' + API_VERSION

class Airtable:
  def __init__(self, app_id, api_key, table):
    self.app_id = app_id
    self.api_key = api_key
    self.table = table

    # build the query url
    self.base_url = API_URL + '/' \
                    + self.app_id + '/' \
                    + self.table

    self.auth_header = {'Authorization': 'Bearer %s' % self.api_key}
    self.records = None

  def get_records(self, parse_data = False):
    records = []
    cntr = 0

    print("Retrieving records from %s table" % self.table)

    while True:
      if cntr == 0:
        params = None
      else:
        params = {'offset': offset}

      req = requests.request('GET', self.base_url,
                              params = params,
                              headers = self.auth_header)

      if req.status_code == requests.codes.ok:
        cntr += 1
        print(" Retrieving batch %d" % cntr)
        req_json = req.json()

        # check for the offset
        if 'offset' in req_json.keys():
          offset = req_json['offset']
        else:
          offset = None

        # Add the records to our list
        recs = req_json['records']
        records = records + recs

        # Break out of the while loop if there is no offset
        if offset is None:
          break
      else:
        print('Error getting records')

    print(" Retrieved %d records" % len(records))
    self.records = records

    if parse_data:
      self.parse_data()

  def parse_data(self):
    # Just return if the records haven't been retrieved
    if self.records is None:
      print("Records haven't been retrieved yet; use get_records method")
      return(None)

    # First, figure out the union of all the record field names
    all_fields = []
    for rec in self.records:
      fields = rec['fields']
      field_names = fields.keys()
      for fn in field_names:
        if fn not in all_fields:
          all_fields.append(fn)

    # Now, read the table data and put into a rectangular form
    # a list of dictionaries, all with the same keys
    # if there is data missing for a particular key, fill in with None
    data = []
    for rec in self.records:
      rec_dict = {}
      fields = rec['fields']

      for fn in all_fields:
        if fn in fields.keys():
          data_val = fields[fn]
        else:
          data_val = None
        rec_dict[fn] = data_val

      data.append(rec_dict)

    self.data = data

