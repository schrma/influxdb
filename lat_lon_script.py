#!/usr/bin/python3

import sys
import argparse
import influxhandler


def main(argv):
    args = parse_arguments(argv)
    influx_client = influxhandler.InfluxHandler("localhost:8086","admin",args.password,"openhab_db") 
    influx_client.write_lat_lon(args.lat,args.lon,args.measurement)

def parse_arguments(command_args):
    """parse the input arguments """
    my_description = 'Cmd write lat and lon to database'
    parser = argparse.ArgumentParser(description=my_description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-l', '--lat', dest='lat',help='Write latitude to influxdb')
    parser.add_argument('-g', '--lon', dest='lon',help='Write longitude to influxdb')
    parser.add_argument('-p', '--password', dest='password',help='Password for influxdb')
    parser.add_argument('-m', '--measurement', dest='measurement',help='Measurement name')
    args = parser.parse_args(command_args)
    return args

if __name__ == "__main__":
   main(sys.argv[1:])