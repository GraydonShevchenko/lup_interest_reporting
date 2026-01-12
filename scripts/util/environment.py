
import arcpy
import os
import sys
import logging
import ctypes

from ctypes import wintypes
from xml.etree import ElementTree as eT
from datetime import datetime as dt



class ArcPyLogHandler(logging.StreamHandler):
    """
    ------------------------------------------------------------------------------------------------------------
        CLASS: Handler used to send logging message to the ArcGIS message window if using ArcMap
    ------------------------------------------------------------------------------------------------------------
    """

    def emit(self, record):
        try:
            msg = record.msg.format(record.args)
        except:
            msg = record.msg

        timestamp = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        if record.levelno == logging.ERROR:
            arcpy.AddError('{} - {}'.format(timestamp, msg))
        elif record.levelno == logging.WARNING:
            arcpy.AddWarning('{} - {}'.format(timestamp, msg))
        elif record.levelno == logging.INFO:
            arcpy.AddMessage('{} - {}'.format(timestamp, msg))

        super(ArcPyLogHandler, self).emit(record)


class Environment:
    """
    ------------------------------------------------------------------------------------------------------------
        CLASS: Contains general environment functions and processes that can be used in python scripts
    ------------------------------------------------------------------------------------------------------------
    """

    def __init__(self):
        pass

    # Set up variables for getting UNC paths
    mpr = ctypes.WinDLL('mpr')

    ERROR_SUCCESS = 0x0000
    ERROR_MORE_DATA = 0x00EA

    wintypes.LPDWORD = ctypes.POINTER(wintypes.DWORD)
    mpr.WNetGetConnectionW.restype = wintypes.DWORD
    mpr.WNetGetConnectionW.argtypes = (wintypes.LPCWSTR,
                                       wintypes.LPWSTR,
                                       wintypes.LPDWORD)

    @staticmethod
    def setup_logger(args):
        """
        ------------------------------------------------------------------------------------------------------------
            FUNCTION: Set up the logging object for message output

            Parameters:
                args: system arguments

            Return: logger object
        ------------------------------------------------------------------------------------------------------------
        """
        log_name = 'main_logger'
        logger = logging.getLogger(log_name)
        logger.handlers = []

        log_fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_file_base_name = os.path.basename(sys.argv[0])
        log_file_extension = 'log'
        timestamp = dt.now().strftime('%Y-%m-%d_%H-%M-%S')
        log_file = '{}_{}.{}'.format(timestamp, log_file_base_name, log_file_extension)

        logger.setLevel(args.log_level)

        sh = logging.StreamHandler()
        sh.setLevel(args.log_level)
        sh.setFormatter(log_fmt)
        logger.addHandler(sh)

        if args.log_dir:
            try:
                os.makedirs(args.log_dir)
            except OSError:
                pass
            
            try:
                fh = logging.FileHandler(os.path.join(args.log_dir, log_file))
                fh.setLevel(args.log_level)
                fh.setFormatter(log_fmt)
                logger.addHandler(fh)
                logger.info('Setting up log file')
            except Exception as e:
                logger.removeHandler(fh)

        if os.path.basename(sys.executable).lower() == 'python.exe':
            arc_env = False
        else:
            arc_env = True

        if arc_env:
            arc_handler = ArcPyLogHandler()
            arc_handler.setLevel(args.log_level)
            arc_handler.setFormatter(log_fmt)
            logger.addHandler(arc_handler)

        return logger


    @staticmethod
    def create_bcgw_connection(location, bcgw_user_name, bcgw_password, db_name='Temp_BCGW.sde', logger=None):
        """
            ------------------------------------------------------------------------------------------------------------
                FUNCTION: Creates a connection object to the bcgw SDE database

                Parameters:
                    location: path to where the database connection object will be saved
                    bcgw_user_name: User name for the BCGW
                    bcgw_password: Password for the BCGW
                    db_name: database name
                    logger: logging object for message output

                Return: None
            ------------------------------------------------------------------------------------------------------------
        """
        if logger:
            logger.info('Connecting to BCGW')

        if not arcpy.Exists(os.path.join(location, db_name)):
            arcpy.CreateDatabaseConnection_management(out_folder_path=location,
                                                      out_name=db_name[:-4],
                                                      database_platform='ORACLE',
                                                      instance='bcgw.bcgov/idwprod1.bcgov',
                                                      username=bcgw_user_name,
                                                      password=bcgw_password,
                                                      save_user_pass='SAVE_USERNAME')
        return os.path.join(location, 'Temp_BCGW.sde')

    @staticmethod
    def delete_bcgw_connection(location, db_name='Temp_BCGW.sde', logger=None):
        """
           ------------------------------------------------------------------------------------------------------------
               FUNCTION: Deletes the bcgw database connection object

               Parameters:
                   location: path to where the database connection object exists
                   db_name: database name
                   logger: logging object for message output

               Return: None
           ------------------------------------------------------------------------------------------------------------
        """
        bcgw_path = os.path.join(location, db_name)
        if logger:
            logger.info('Deleting BCGW connection')
        if location == 'Database Connections':
            os.remove(Environment.sde_connection(db_name))
        else:
            os.remove(bcgw_path)

    @staticmethod
    def sde_connection(db_name):
        """
            ------------------------------------------------------------------------------------------------------------
                FUNCTION: get the network path of an sde connection if its in Database Connections

                Parameters:
                    db_name: name of the database to look for

                Return str: path of the sde connection file
            ------------------------------------------------------------------------------------------------------------
        """
        appdata = os.getenv('APPDATA')
        arcgisVersion = arcpy.GetInstallInfo()['Version'][:-2] \
            if arcpy.GetInstallInfo()['Version'].count('.') > 1 else arcpy.GetInstallInfo()['Version']
        arcCatalogPath = os.path.join(appdata, 'ESRI', u'Desktop' + arcgisVersion, 'ArcCatalog')

        for f in os.listdir(arcCatalogPath):
            fileIsSdeConnection = f.lower().endswith(".sde")
            if fileIsSdeConnection and f == db_name:
                return os.path.join(arcCatalogPath, f)
