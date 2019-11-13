import argparse
from influxdb import InfluxDBClient

class InfluxHandler:
    def __init__(self,servername,user,password,dbname):
        host = servername[0:servername.rfind(':')] 
        port = int(servername[servername.rfind(':')+1:])
        self.client = InfluxDBClient(host, port, user, password, dbname)
 
    def delete_database(self,dbname):
        self.client.drop_database(dbname)
    
    def read_database(self,serie,timeinterval):
        query_string = "SELECT * FROM {} WHERE {}".format(serie,timeinterval)
        result = self.client.query(query_string)
        if result.error:
            raise RuntimeError('query failed')
        all_values = list(result.get_points(measurement=serie))
        return all_values


    def write_database_for_lat_lon(self, inputdata,measurement_name):
        datapoints=[]
        for single_value in inputdata:
            string_value = single_value["value"].split(',')
            fields={"lat": float(string_value[0]), "lon" : float(string_value[1])}
            point = {"measurement": measurement_name, "time": single_value["time"], "fields": fields}
            datapoints.append(point)
        response = self.client.write_points(datapoints)
        if not response:
            print('Problem inserting points, exiting...')
            exit(1)

        print("Wrote %d, response: %s" % (len(datapoints), response))
    
    def write_lat_lon(self, lat, lon, measurement_name):
        fields={"lat": float(lat), "lon" : float(lon)}
        point = {"measurement": measurement_name, "fields": fields}
        datapoints = []
        datapoints.append(point)
        response = self.client.write_points(datapoints)
        if not response:
            print('False')
        print("True")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Csv to influxdb.')


    parser.add_argument('-i', '--input', nargs='?', required=True,
                        help='Input csv file.')

    parser.add_argument('-d', '--delimiter', nargs='?', required=False, default=',',
                        help='Csv delimiter. Default: \',\'.')

    parser.add_argument('-s', '--server', nargs='?', default='localhost:8086',
                        help='Server address. Default: localhost:8086')

    parser.add_argument('-u', '--user', nargs='?', default='root',
                        help='User name.')

    parser.add_argument('-p', '--password', nargs='?', default='root',
                        help='Password.')

    parser.add_argument('--dbname', nargs='?', required=True,
                        help='Database name.')

    parser.add_argument('--create', action='store_true', default=False,
                        help='Drop database and create a new one.')

    parser.add_argument('-m', '--metricname', nargs='?', default='value',
                        help='Metric column name. Default: value')

    parser.add_argument('-tc', '--timecolumn', nargs='?', default='timestamp',
                        help='Timestamp column name. Default: timestamp.')

    parser.add_argument('-tf', '--timeformat', nargs='?', default='%Y-%m-%d %H:%M:%S',
                        help='Timestamp format. Default: \'%%Y-%%m-%%d %%H:%%M:%%S\' e.g.: 1970-01-01 00:00:00')

    parser.add_argument('-tz', '--timezone', default='UTC',
                        help='Timezone of supplied data. Default: UTC')

    parser.add_argument('--fieldcolumns', nargs='?', default='value',
                        help='List of csv columns to use as fields, separated by comma, e.g.: value1,value2. Default: value')

    parser.add_argument('--tagcolumns', nargs='?', default='host',
                        help='List of csv columns to use as tags, separated by comma, e.g.: host,data_center. Default: host')

    parser.add_argument('-g', '--gzip', action='store_true', default=False,
                        help='Compress before sending to influxdb.')

    parser.add_argument('-b', '--batchsize', type=int, default=5000,
                        help='Batch size. Default: 5000.')

    args = parser.parse_args()

    influx_client = InfluxHandler(args.server,args.user,args.password,args.dbname) 
    #r = influx_client.read_database("StringLocationMarco","time > now() - 2h")
    #influx_client.write_database_for_lat_lon(r,"LocationMarco1")
    influx_client.write_lat_lon(10.2,9.2,"Loc")
    #influx_client.delete_database(args.dbname)

    #loadCsv(args.input, args.server, args.user, args.password, args.dbname, 
        #args.metricname, args.timecolumn, args.timeformat, args.tagcolumns, 
        #args.fieldcolumns, args.gzip, args.delimiter, args.batchsize, args.create, 
        #args.timezone)
