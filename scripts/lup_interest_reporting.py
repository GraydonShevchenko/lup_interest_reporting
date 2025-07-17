# Filename: ce_overlap.py
# Author: Graydon Shevchenko
# Created: April 22, 2025
# Description: This script is used to report out on cumulative effects values and where they intersect with a defined area of interest

import sys
import os
import logging
import arcpy
import pandas
import warnings
import openpyxl
import shutil
import re

from copy import copy
from openpyxl.styles import Border, Side, PatternFill, Font, Alignment, NamedStyle
from openpyxl.utils import get_column_letter
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime

import arcpy.management

from util.environment import Environment


def run_app() -> None:
    """
    FUNCTION

    run_app: Used to control the general flow of the script 
    """

    # Gather the input parameters and set up the class oject
    file_num, out_dir, xls, aoi, aoi_fld, leave, python_dir, logger = get_input_parameters()
    ce_overlaps = CE_Overlaps(file_number=file_num, output_dir=out_dir, xls_schema=xls, aoi=aoi, aoi_field=aoi_fld,
                            leave_areas=leave, python_dir=python_dir, logger=logger)
    
    # Run the class methods needed to perform the overlap assessment
    ce_overlaps.setup_aoi()
    ce_overlaps.overlay_values()
    ce_overlaps.write_excel()

    # Delete the class object
    del ce_overlaps


def get_input_parameters() -> tuple:
    """
    FUNCTION

    get_input_parameters: Uses the argparse library to gather the input parameters and set up the logging object

    Returns:
        tuple: file name/number, output directory, excel schema, area of interest feature layer, area of interest field, leave areas feature layer, directory the script is run from, logger object
    """

    try:
        # Set up the input parameters. This matches what is put into the toolbox tool within ArcGIS.  Doing this allows for running the script within command line
        parser = ArgumentParser(description='This script is used to calculate cumulative effects values based on an '
                                            'input area of interest. Regional based CE values can be used by inputting a schema using an excel file')
        parser.add_argument('file_num', type=str, help='File Name or number')
        parser.add_argument('out_dir', type=str, help='Output directory')
        parser.add_argument('xls', type=str, help='Input schema Excel file')
        parser.add_argument('aoi', type=str, help='Area of interest layer')
        parser.add_argument('aoi_fld', type=str, help='AOI ID field')
        parser.add_argument('leave', type=str, help='AOI leave areas')
        parser.add_argument('--log_level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                            help='Log level for message output')
        parser.add_argument('--log_dir', help='Path to the log file directory')

        args = parser.parse_args()

        # Set up the logger object
        logger = Environment.setup_logger(args)

        python_dir = os.path.dirname(sys.argv[0])

        return args.file_num, args.out_dir, args.xls, args.aoi, args.aoi_fld, args.leave, python_dir, logger

    except Exception as e:
        logging.exception(f'Unexpected exception, program terminating: {repr(e)}')


class CE_Overlaps:
    """
    CLASS

    Main class object that contains the methods required to complete the cumulative effects framework analysis
    """
    def __init__(self, file_number:str, output_dir:str, xls_schema:str, aoi:str, aoi_field:str, leave_areas:str, 
                 python_dir:str, logger:logging.Logger) -> None:
        """
        CLASS METHOD

        __init__: Initialization function that is run when this class object is created.  Its sets up the class variables required as well as runs initial processes to set up the workspace and read through the excel schema
        Args:
            file_number (str): the file name or number which is used to help name the output file and geodatabase
            output_dir (str): directory where the script outputs will be stored
            xls_schema (str): path to the excel schema document
            aoi (str): area of interest feature layer
            aoi_field (str): area of interest field
            leave_areas (str): leave areas feature layer
            python_dir (str): directory where the script is run from
            logger (logging.Logger): logger object for messaging
        """

        # Write the parameters into the class variables
        self.file_number = file_number.replace(' ', '_')
        self.output_dir = output_dir
        self.xls_schema = xls_schema
        self.aoi = aoi
        self.fld_aoi = aoi_field if aoi_field != '#' else None
        self.leave_areas = leave_areas if leave_areas != '#' else None
        self.python_dir = python_dir
        self.logger = logger

        # Set up the ce dictionary for storing the values
        self.dict_ce_values = defaultdict(lambda: defaultdict(lambda: CE_Value))

        # File paths for spatial files, connections and output files
        self.bcgw = os.path.join(self.python_dir, 'util', 'BCGW_Connection.sde')
        self.out_gdb = os.path.join(self.output_dir, f'ce_data_{self.file_number}.gdb')
        self.fd_incoming = os.path.join(self.out_gdb, 'incoming')
        self.fd_work = os.path.join(self.out_gdb, 'work')

        self.fc_aoi = os.path.join(self.fd_incoming, 'aoi')
        self.fc_leave_areas = os.path.join(self.fd_incoming, 'leave_areas')
        self.fc_net_aoi = os.path.join(self.fd_incoming, 'net_aoi')

        self.aoi_total = 0
        self.dict_aoi_area = defaultdict(int)

        self.xls_output = os.path.join(self.output_dir, 
                                      f'CE_Overview_{self.file_number}_{datetime.strftime(datetime.today(), "%Y%m%d")}.xlsx')

        # Other variables required for running the script
        self.xl_style = None
        self.str_overall = 'Overall'

        self.fld_area = 'AreaHA'

        # Standard headers to inlcude in the excel output for every ce value
        self.standard_headers = ['Assessment Unit', 'Area (ha) of Assessment Unit', 
                                 'Area (ha) of AOI Overlap with Assessment Unit', 
                                 '% of AOI that Overlaps with Assessment Unit', 
                                 '% of Assessment Unit that Overlaps with AOI']

        arcpy.env.overwriteOutput = True
        warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


        # Set up the output geodatabase in the specified output directory
        try:
            self.logger.info(f'Setting up geodatabase {os.path.basename(self.out_gdb)}')
            arcpy.management.CreateFileGDB(out_folder_path=os.path.dirname(self.out_gdb), 
                                           out_name=os.path.basename(self.out_gdb))
        except:
            pass

        # Create the incoming and working feature datasets within the new geodatabase
        self.logger.info('Adding in feature datasets')
        if arcpy.Exists(self.fd_incoming):
            arcpy.management.Delete(in_data=self.fd_incoming)
        try:
            arcpy.management.CreateFeatureDataset(out_dataset_path=os.path.dirname(self.fd_incoming), 
                                                  out_name=os.path.basename(self.fd_incoming), spatial_reference=3005)
        except:
            pass
        if arcpy.Exists(self.fd_work):
            arcpy.management.Delete(in_data=self.fd_work)
        try:
            arcpy.management.CreateFeatureDataset(out_dataset_path=os.path.dirname(self.fd_work), 
                                                  out_name=os.path.basename(self.fd_work), spatial_reference=3005)
        except:
            pass

        # Copy the area of interest and leave area feature layers to the geodatabase if they exist
        self.logger.info('Copying data to working geodatabase')
        if self.aoi:
            self.logger.info(f'Copying aoi: {os.path.basename(self.aoi)}')
            arcpy.management.CopyFeatures(in_features=self.aoi, out_feature_class=self.fc_aoi)

        if self.leave_areas:
            self.logger.info(f'Copying leave areas: {os.path.basename(self.leave_areas)}')
            arcpy.management.CopyFeatures(in_features=self.leave_areas, out_feature_class=self.fc_leave_areas)
        

        # Set the processing extent to the area of interest, helps with exporting only needed features instead of the whole input dataset
        arcpy.env.extent = self.fc_aoi

        # Read in the schema values from the input excel file
        self.logger.info('Reading in ce values from excel schema')
        try:
            ce_df = pandas.read_excel(self.xls_schema, sheet_name='CE Indicators', engine='openpyxl', skiprows=[1], 
                                    na_filter=False)
        except:
            self.logger.error('Could not read in the excel schema as it does not contain the CE Indicators sheet')
            sys.exit()

        try:
            # Loop through each row and gather the ce value information
            for row in ce_df.itertuples():
                # Check if any of the required fields are empty, if so, then skip over it
                if any([not i for i in [row.CE_VALUE, row.VALUE_TYPE, row.DATASET_NAME, row.ASSESSMENT_YEAR, 
                            row.PATH]]):
                    self.logger.warning(f'The value {row.CE_VALUE} is missing one or more of the required fields; skipping this value')
                    continue
                # Check the indicated path object exists, if not it may be a BCGW path
                if arcpy.Exists(row.PATH):
                    full_path = row.PATH
                else:
                    # Check if the BGW path exists, if not, then skip using the value in the report
                    if arcpy.Exists(os.path.join(self.bcgw, row.PATH)):
                        full_path = os.path.join(self.bcgw, row.PATH)
                    else:
                        self.logger.warning(f'!!! Could not find the path specified in the excel: {row.PATH}. Skipping this     value')
                        continue
                
                try:
                    unique_id = row.UNIQUE_ID_FIELD
                except:
                    unique_id = None


                # Create a CE Value object with the gathered information and add it to the dictionary
                ce_value = CE_Value(name=row.DATASET_NAME, value_type=row.VALUE_TYPE, assess_year=row.ASSESSMENT_YEAR, 
                                    path=full_path, unique_field=unique_id, assess_field=row.ASSESSMENT_UNIT_FIELD, sql=row.SQL, source_field=row.SOURCE_FIELD, join_table_path=row.JOIN_TABLE_PATH, join_table_field=row.JOIN_TABLE_FIELD)
                self.dict_ce_values[row.CE_VALUE][row.DATASET_NAME] = ce_value
        except Exception as e:
            # Exit out of the script if there is an error; most likely caused by an incorrect schema file used
            self.logger.error(f'There was an issue with the input schema dataset used.  Ensure all the required fields exist within the excel sheet.  Stack Trace: {repr(e)}')
            sys.exit()

        # Read in the values from the additional field schema page. This includes formatting and values used for each indicatory on each specified dataset
        try:
            self.logger.info('Reading in additional field schemas')
            wb = openpyxl.load_workbook(filename=self.xls_schema)
            ws = wb['Additional Fields']
        except:
            self.logger.error('Could not read in the excel schema as it does not contain the Additional Fields sheet')
            sys.exit()

        # Loop through each row of the excel sheet
        for row in ws.iter_rows(min_row=3, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
            # Pull in the row information into a Field Schema object
            ds_name = row[0].value
            fld_schema = FieldSchema()
            fld_schema.name = row[1].value
            fld_schema.label = row[2].value if row[2].value else row[1].value
            fld_schema.other_fields = row[3].value.replace(' ','').split(',') if row[3].value else []
            fld_schema.value_type = row[4].value

            # Loop through the columns containing the break values and formatting for each ce value
            for i in range(5, len(row)):
                if not row[i].value:
                    break
                # Set up a Value Schema object
                cell_schema = ValueSchema()
                cell_value = row[i].value

                # If the value type specified is a range, then parse the values and place them in the value schema object
                if fld_schema.value_type == 'Range':
                    # Pull out the numerical values from the string and place them in a list.  This is used to extract ranges that are indicated using the format low value-high value (eg. 0.5-1.2)
                    lst_range = re.findall(r'-?\d+\.?\d*--?\d+\.?\d*', cell_value.replace(' ',''))
                    range_operator = None

                    # If the extracted values are in fact a range, then place them in applicable variables
                    if lst_range:
                        for rng in lst_range:
                            low_val = rng.split('-')[0]
                            high_val = rng.split('-')[1]
                            break
                    # If it is not a standard range, then search for the comparison operators and parse out appropriately
                    else:
                        range_val = cell_value.replace(' ','')
                        if '<=' in range_val:
                            low_val = None
                            high_val = range_val.replace('<=','')
                            range_operator = '<='
                        elif '>=' in range_val:
                            low_val = range_val.replace('>=','')
                            high_val = None
                            range_operator = '>='
                        elif '<' in range_val:
                            low_val = None
                            high_val = range_val.replace('<','')
                            range_operator = '<'
                        elif '>' in range_val:
                            low_val = range_val.replace('>','')
                            high_val = None
                            range_operator = '>'

                    # Account for percent values
                    low_val = None if not low_val else float(low_val) if '%' not in low_val else float(low_val.replace('%',''))/100
                    high_val = None if not high_val else float(high_val) if '%' not in high_val else float(high_val.replace('%',''))/100

                    # Add to the cell schema object
                    cell_schema.range_low = low_val
                    cell_schema.range_high = high_val
                    cell_schema.range_operator = range_operator

                # Pull out the cell formatting and place in the cell schema object
                cell_schema.style_align = copy(row[i].alignment)
                cell_schema.style_border = copy(row[i].border)
                cell_schema.style_fill = copy(row[i].fill)
                cell_schema.style_font = copy(row[i].font)
                cell_schema.style_format = copy(row[i].number_format)

                # Add the cell schema object to the field schema object
                fld_schema.dict_values[cell_value] = cell_schema

            # Find the appropriate dataset in the ce values dictionary and add in the field schema object
            for ce in self.dict_ce_values:
                for ds in self.dict_ce_values[ce]:
                    if ds ==  ds_name:
                        self.dict_ce_values[ce][ds].other_fields_schema[fld_schema.name] = fld_schema
        wb.close()


    def setup_aoi(self) -> None:
        """
        CLASS METHOD

        setup_aoi: Sets up the area of interest datset and removes any leave areas if required
        """
        # Add in the total area field and calcualte the area in hectares for each feature
        self.logger.info('Determining aoi area in hectares')
        arcpy.management.AddField(in_table=self.fc_aoi, field_name=self.fld_area, field_type='DOUBLE')
        with arcpy.da.UpdateCursor(in_table=self.fc_aoi, field_names=[self.fld_area, 'SHAPE@AREA']) as u_cursor:
            for row in u_cursor:
                row[0] = row[1]/10000
                u_cursor.updateRow(row)

        # If the leave areas exist, then erase them out of the area of interest.  Uses the union tool as the Erase requires an Advanced licence
        if arcpy.Exists(self.fc_leave_areas):

            self.logger.info('Removing leave areas')
            # Run the union of the aoi and leave areas
            arcpy.analysis.Union(in_features=[self.fc_aoi, self.fc_leave_areas], out_feature_class=self.fc_net_aoi)

            with arcpy.da.UpdateCursor(self.fc_net_aoi, 
                                       field_names=[self.fld_area, 'FID_leave_areas', 'SHAPE@AREA']) as u_cursor:
                # Loop through each record and delete it if it is covered by the leave area dataset
                for row in u_cursor:
                    if row[1] != -1:
                        u_cursor.deleteRow()
                        continue
                    # Update the area field
                    row[0] = row[2]/10000
                    u_cursor.updateRow(row)

            # Delete any fields created as a product of the union
            lst_aoi_fields = [fld.name for fld in arcpy.ListFields(dataset=self.fc_aoi)]
            lst_net_fields = [fld.name for fld in arcpy.ListFields(dataset=self.fc_net_aoi)]
            lst_drop_fields = list(set(lst_net_fields) - set(lst_aoi_fields))
            arcpy.management.DeleteField(in_table=self.fc_net_aoi, drop_field=lst_drop_fields)
        else:
            # Copy the aoi if no leave areas were specified
            arcpy.management.CopyFeatures(in_features=self.fc_aoi, out_feature_class=self.fc_net_aoi)

        # Loop through the net aoi dataset and add the total areas to a dictionary for later use
        lst_fields = ['SHAPE@AREA']
        if self.fld_aoi:
            lst_fields.append(self.fld_aoi)
        with arcpy.da.SearchCursor(in_table=self.fc_net_aoi, field_names=lst_fields) as s_cursor:
            for row in s_cursor:
                self.aoi_total += row[0] / 10000
                if self.fld_aoi:
                    self.dict_aoi_area[row[1]] += row[0] / 10000


    def overlay_values(self) -> None:
        """
        CLASS METHOD

        overlay_values: Main function that performs the overlays between the aoi and the ce values, then stores the results in the dictionary
        """

        # Loop through the ce values extracted from the input excel schema file
        for ce in self.dict_ce_values:
            # Loop through the datasets for each ce value
            for ds in self.dict_ce_values[ce]:

                self.logger.info(f'Running overlay on {ds}')
                ce_ds = self.dict_ce_values[ce][ds]
                in_fc = ce_ds.path
                join_fc = ce_ds.join_path

                # Check if the dataset overlaps the aoi, if not, then continue on to the next dataset
                if not self.check_overlaps(fc_one=self.fc_net_aoi, fc_two=in_fc):
                    self.logger.warning('***Value does not overlap the area of interest***')
                    continue

                # Extrapolate the union id field name based on the input dataset
                fld_id_aoi = f'FID_{arcpy.Describe(self.fc_net_aoi).name}'
                ce_name = str(arcpy.Describe(in_fc).name).replace(".shp","")
                if '.' in ce_name:
                    ce_name = ce_name.split('.')[1]
                fld_id_ce = f'FID_{ce_name}'

                ds_name = re.sub('[^a-zA-Z0-9\n\.]','_', ce_ds.name)

                # Union the net aoi and the value dataset together, then add the path to the values dictionary
                out_fc = os.path.join(self.fd_work, f'union_{ds_name.lower()}')
                arcpy.analysis.Union(in_features=[self.fc_net_aoi, in_fc], out_feature_class=out_fc)
                self.dict_ce_values[ce][ds].fc_union = out_fc

                # If there is a join tables specifed in the schema for this value then, join it to the unioned dataset
                if join_fc:
                    # If the join fields were not specified correctly in the excel schema, then don't perform the join
                    if not any([ce_ds.source_field, ce_ds.join_field]):
                        self.logger.warning('The fields required for the table join were not specified in the schema')
                    else:
                        self.logger.info(f'Joining {os.path.basename(join_fc)} to dataset')
                        try:
                            # Copy the specified table to the geodatabase then join to the unioned dataset
                            join_tbl = os.path.join(self.out_gdb, f'{os.path.basename(out_fc)}_jointable')
                            arcpy.management.CopyRows(in_rows=join_fc, out_table=join_tbl)
                            lst_fields = [fld.name for fld in arcpy.ListFields(join_tbl) if fld.name not in 
                                          ['OBJECTID', 'Shape_Area', 'Shape_Length', 'Shape']]
                            arcpy.management.JoinField(in_data=out_fc, in_field=ce_ds.source_field, 
                                                       join_table=join_tbl, join_field=ce_ds.join_field, fields=lst_fields)
                        except:
                            self.logger.warning('Something went wrong with the join, could not complete the operation')
                
                # Compile the list of fields to search the unioned dataset based on the input schema
                lst_fields = ['SHAPE@AREA', fld_id_aoi, fld_id_ce] + \
                                (list(set(ce_ds.assessment_fields) - set(ce_ds.id_fields))) + ce_ds.id_fields
                lst_additional = []
                if ce_ds.other_fields_schema:
                    for fld in ce_ds.other_fields_schema:
                        lst_additional.extend([fld] + ce_ds.other_fields_schema[fld].other_fields)
                    lst_fields.extend(lst_additional)
                if self.fld_aoi:
                    lst_fields.append(self.fld_aoi)

                lst_intersecting_aus = []
                # Gather a list of the assessment units that intersect the area of interest
                with arcpy.da.SearchCursor(in_table=out_fc, field_names=lst_fields, where_clause=ce_ds.sql) as s_cursor:
                    for row in s_cursor:
                        fid_aoi = row[lst_fields.index(fld_id_aoi)]
                        lst_au = []
                        for fld in ce_ds.id_fields:
                            if str(row[lst_fields.index(fld)]) not in ['','None']:
                                lst_au.append(str(row[lst_fields.index(fld)]))
                        if not lst_au:
                            continue
                        au = ' '.join(lst_au)
                        if row[lst_fields.index(fld_id_aoi)] != -1:
                            lst_intersecting_aus.append(au)
                # Flag used to determine if the SQL clause resulted in no records returned
                bl_no_rows = True

                # Loop through the records in the unioned resultant and pull out the required attributes
                with arcpy.da.SearchCursor(in_table=out_fc, field_names=lst_fields, where_clause=ce_ds.sql) as s_cursor:
                    for row in s_cursor:

                        bl_no_rows = False
                        shp = row[0]/10000
                        fid_aoi = row[lst_fields.index(fld_id_aoi)]
                        fid_ce = row[lst_fields.index(fld_id_ce)]
                        aoi = None if not self.fld_aoi else row[lst_fields.index(self.fld_aoi)]
                        lst_au = [] if ce_ds.id_fields else ['All Units']
                        lst_au_name = [] if ce_ds.assessment_fields else ['All Units']

                        # Create the assessment unit id; takes into account if more than one field was selected
                        for fld in ce_ds.id_fields:
                            if str(row[lst_fields.index(fld)]) not in ['', 'None']:
                                lst_au.append(str(row[lst_fields.index(fld)]))
                        if not lst_au:
                            continue
                        else:
                            au = ' '.join(lst_au)

                        # Skip the record if the assessment unit is not in the list of intersecting ones
                        if au not in lst_intersecting_aus and au != 'All Units':
                            continue

                        # Create the assessment unit name; takes into account if more than one field was selected
                        for fld in ce_ds.assessment_fields:
                            if str(row[lst_fields.index(fld)]) not in ['', 'None']:
                                lst_au_name.append(str(row[lst_fields.index(fld)]))
                            else:
                                lst_au_name.append('Unnamed')

                        au_name = ' '.join(lst_au_name)

                        # Incrememnt total area of the assessment unit for overall and the aoi if an aoi field was selected
                        self.dict_ce_values[ce][ds].aoi[self.str_overall].assessment_units[au].au_name = au_name
                        self.dict_ce_values[ce][ds].aoi[self.str_overall].assessment_units[au].total_area += shp
                        if aoi:
                            self.dict_ce_values[ce][ds].aoi[aoi].assessment_units[au].au_name = au_name
                            self.dict_ce_values[ce][ds].aoi[aoi].assessment_units[au].total_area += shp

                        # If the feature is within the area of interest
                        if fid_aoi != -1:
                            
                            # Incrememnt total area of the aoi for overall and the aoi if an aoi field was selected
                            self.dict_ce_values[ce][ds].aoi[self.str_overall].total_area += shp
                            if aoi:
                                self.dict_ce_values[ce][ds].aoi[aoi].total_area += shp

                            # If the feature is within the ce value
                            if fid_ce != -1:
                                # Incrememnt area of the aoi within the assessment unit for overall
                                self.dict_ce_values[ce][ds].aoi[self.str_overall].assessment_units[au].aoi_area += shp

                                # Loop through the additional fields and add the values to the dictionary for overall
                                for o_fld in lst_additional:
                                    self.dict_ce_values[ce][ds].aoi[self.str_overall].assessment_units[au].other_fields[o_fld] = row[lst_fields.index(o_fld)] if row[lst_fields.index(o_fld)] else ''

                                # If an aoi field was selected, increment the aoi area within the assessment unit
                                if aoi:
                                    self.dict_ce_values[ce][ds].aoi[aoi].assessment_units[au].aoi_area += shp
                                    # Loop through the additional fields and add the values to the dictionary for the aoi
                                    for o_fld in lst_additional:
                                        self.dict_ce_values[ce][ds].aoi[aoi].assessment_units[au].other_fields[o_fld] = row[lst_fields.index(o_fld)] if row[lst_fields.index(o_fld)] else ''

                # If the flag was not lowered, output warning of no overlap                        
                if bl_no_rows:
                    self.logger.warning(f'+++Value does not overlap the area of interest with the SQL clause: {ce_ds.sql}+++')


    def write_excel(self) -> None:
        """
        CLASS METHOD

        write_excel: Function that is used to produce the excel analysis report based on the information found from the overlays, and using the formatting options laid out in the schems document
        """

        # Check if the output excel file already exists, delete if it does
        self.logger.info('Writing excel output')
        if os.path.exists(self.xls_output):
            os.remove(self.xls_output)

        # Copy the schema excel to the new location and remove all sheets but the front page
        shutil.copyfile(self.xls_schema, self.xls_output)
        wb = openpyxl.load_workbook(self.xls_output)
        for sheet in wb.sheetnames:
            if sheet != 'Front Page':
                wb.remove(worksheet=wb[sheet])


        # Set up the standard styles and add to the workbook
        self.xl_style = ExcelStyles(wb=wb)
        lst_sheets = [self.str_overall]

        # Add aoi names to the sheet list if there is more than one unique aoi
        if len(self.dict_aoi_area.keys()) > 1:
            lst_sheets.extend([k for k in self.dict_aoi_area])


        # Loop through the list of sheets to create
        for sheet in lst_sheets:

            # Add a new sheet with the specified name and set the worksheet opject to point to it
            self.logger.info(f'Writing sheet - {sheet}')
            wb.create_sheet(title=str(sheet))
            ws = wb[str(sheet)]

            # Row and column index objects
            i_row = 1
            i_col = 1

            # Write the standardized title information to the top of the sheet
            self.logger.info('Setting up title information')
            title_text = f'Cumulative Effects Analysis - {os.path.basename(self.aoi)}'
            ws.cell(row=i_row, column=i_col, value=title_text).style = self.xl_style.title
            ws.merge_cells(start_row=i_row, start_column=i_col, end_row=i_row, end_column=i_col + 5)
            i_row +=1
            ws.cell(row=i_row, column=i_col, value='File Name/Number:').style = self.xl_style.regular_left
            ws.cell(row=i_row, column=i_col+1, value=self.file_number).style = self.xl_style.regular
            i_row +=1
            ws.cell(row=i_row, column=i_col, value='Date Submitted:').style = self.xl_style.regular_left
            ws.cell(row=i_row, column=i_col+1, value='').style = self.xl_style.regular
            i_row +=1
            ws.cell(row=i_row, column=i_col, value='Submitter Name:').style = self.xl_style.regular_left
            ws.cell(row=i_row, column=i_col+1, value='').style = self.xl_style.regular
            i_row +=1
            ws.cell(row=i_row, column=i_col, value='Email:').style = self.xl_style.regular_left
            ws.cell(row=i_row, column=i_col+1, value='').style = self.xl_style.regular
            i_row +=1
            ws.cell(row=i_row, column=i_col, value='Ministry/Organization:').style = self.xl_style.regular_left
            ws.cell(row=i_row, column=i_col+1, value='').style = self.xl_style.regular
            i_row +=1
            ws.cell(row=i_row, column=i_col, value='Net AOI Area (ha)*:').style = self.xl_style.regular_left
            ws.cell(row=i_row, column=i_col+1, value='').style = self.xl_style.regular

            # Use the overall area if the sheet is overall, otherwise use the area value for the specific aoi part
            if sheet == self.str_overall:
                aoi_value = self.aoi_total
            else:
                aoi_value = self.dict_aoi_area[sheet]
            ws.cell(row=i_row, column=i_col+1, value=aoi_value).style = self.xl_style.number
            aoi_row = i_row
            ws.cell(row=i_row, column=i_col+2, 
                    value='*If spatially explicit leave areas are provided they are netted out of the AOI total area    (removed). Otherwise, net area = gross area').style = self.xl_style.italics
            ws.merge_cells(start_row=i_row, start_column=i_col+2, end_row=i_row, end_column=i_col + 7)

            i_row +=2

            # Loop through the ce values in the dictionary
            for ce in self.dict_ce_values:
                self.logger.info(f'Writing {ce} results')
                
                # Loop through the datasets for the specified ce value
                for ds in self.dict_ce_values[ce]:

                    # Gather fields, labels and schema for the specific dataset
                    ce_ds = self.dict_ce_values[ce][ds]
                    ce_fields = list(ce_ds.other_fields_schema.keys())
                    other_headers = [ce_ds.other_fields_schema[fld].label for fld in ce_ds.other_fields_schema]
                    ce_headers = [self.standard_headers[0]] + other_headers + self.standard_headers[1:]
                    add_index = i_col + len(ce_fields)

                    # Write the header text
                    header_text = f'Cumulative Effects Framework {self.dict_ce_values[ce][ds].value_type} Value - ' \
                            f'{ce} ({self.dict_ce_values[ce][ds].assessment_year})'
                    ws.cell(row=i_row, column=i_col, value=header_text).style = self.xl_style.value_header
                    ws.merge_cells(start_row=i_row, start_column=i_col, end_row=i_row, 
                                   end_column=len(ce_headers))
                    
                    # Add in a dataset sub header if there is more than one dataset used in the value
                    i_row += 1
                    if len(self.dict_ce_values[ce]) > 1:
                        ws.cell(row=i_row, column=i_col, value=ds).style = self.xl_style.value_subheader
                        ws.merge_cells(start_row=i_row, start_column=i_col, end_row=i_row, 
                                       end_column=len(ce_headers))
                        i_row += 1

                    # Write the standardized column headers and any additional ones specified in the schema
                    for header in ce_headers:                           
                        ws.cell(row=i_row, column=i_col + ce_headers.index(header), 
                                value=header).style = self.xl_style.column_header
                    i_row += 1

                    # If there was no overlap with the aoi, then indicate as such in the sheet and mvoe on to the next dataset
                    if len(ce_ds.aoi[sheet].assessment_units) == 0:
                        ws.cell(row=i_row, column=i_col, value=f'No overlap with {ds}').style = self.xl_style.regular
                        ws.merge_cells(start_row=i_row, start_column=i_col, end_row=i_row, 
                                       end_column=len(ce_headers))
                        i_row += 2
                        continue
                        
                    # Loop through the assessment units for the dataset
                    for au in ce_ds.aoi[sheet].assessment_units:

                        assess_unit = ce_ds.aoi[sheet].assessment_units[au]
                        
                        # Write the standardized values in the first 5 columns
                        ws.cell(row=i_row, column=i_col, value=assess_unit.au_name).style=self.xl_style.regular
                        ws.cell(row=i_row, column=add_index+1, 
                                value=ce_ds.aoi[self.str_overall].assessment_units[au].total_area).style=self.xl_style.number
                        ws.cell(row=i_row, column=add_index+2, value=assess_unit.aoi_area).style=self.xl_style.number
                        ws.cell(row=i_row, column=add_index+3, 
                                value=f'=${get_column_letter(add_index+2)}${i_row}/${get_column_letter(i_col+1)}${aoi_row}').style=self.xl_style.percent
                        ws.cell(row=i_row, column=add_index+4, 
                                value=f'=${get_column_letter(add_index+2)}${i_row}/${get_column_letter(add_index+1)}${i_row}').style=self.xl_style.percent

                        # Loop through the additional fields for the dataset
                        for ce_fld in ce_fields:
                            # Pull the values from the schema object
                            ce_schema = ce_ds.other_fields_schema[ce_fld]
                            col_index = ce_headers.index(ce_schema.label) + 1
                            try:
                                ce_value = ce_ds.aoi[sheet].assessment_units[au].other_fields[ce_fld]
                            except:
                                continue

                            # Convert to a float rounded to two decimal places if the value is a decimal
                            try:
                                flt_value = float(ce_value)
                                ce_value = round(ce_value, 3) if '.' in str(ce_value) else ce_value
                            except:
                                pass
                            ws.cell(row=i_row, column=col_index, value=ce_value)

                            # If there isn't a value, then keep the cell style as regular
                            if not ce_value:
                                ws.cell(row=i_row, column=col_index).style = self.xl_style.regular
                                continue

                            # Gather the other field information and add it within brackets after the main value
                            if ce_schema.other_fields:
                                lst_other_values = [ce_ds.aoi[sheet].assessment_units[au].other_fields[o_fld] for o_fld in ce_schema.other_fields]
                                join_val = f'{ws.cell(row=i_row, column=col_index).value} ({",".join(lst_other_values)})'
                                ws.cell(row=i_row, column=col_index, value=join_val)

                            # Pull the formatting values from the schema object and create the cell style
                            cell_style = self.xl_style.regular
                            if ce_schema.value_type:
                                # If the value type is discrete, pull the values as is based on the data
                                if ce_schema.value_type == 'Discrete':
                                    val_style = ce_schema.dict_values[ce_value]
                                
                                # If the value type is a range, then cycle through the different range types and assign the format if it meets the requirements
                                elif ce_schema.value_type == 'Range':
                                    for val_key in ce_schema.dict_values:
                                        val_schema = ce_schema.dict_values[val_key]
                                        if val_schema.range_low and val_schema.range_high:
                                            if val_schema.range_low <= ce_value <= val_schema.range_high:
                                                break
                                        elif not val_schema.range_low and val_schema.range_high:
                                            if val_schema.range_operator == '<=' and ce_value <= val_schema.range_high:
                                                break
                                            elif val_schema.range_operator == '<' and ce_value < val_schema.range_high:
                                                break
                                        elif val_schema.range_low and not val_schema.range_high:
                                            if val_schema.range_operator == '>=' and ce_value >= val_schema.range_low:
                                                break
                                            elif val_schema.range_operator == '>' and ce_value > val_schema.range_low:
                                                break
                                    val_style = val_schema
                            
                                # Create a new style object based on the extracted formats, then assign it to the cell
                                cell_style = self.xl_style.create_style_copy(wb=wb, 
                                                                             name=f'{ce}-{ds}-{sheet}-{ce_fld}-{ce_value}', font=val_style.style_font, 
                                                                             align=val_style.style_align, border=val_style.style_border, fill=val_style.style_fill, num_format=val_style.style_format)
                            
                            ws.cell(row=i_row, column=col_index).style = cell_style

                        i_row += 1
                    i_row += 1

            # Set the column widths
            ws.column_dimensions[get_column_letter(i_col)].width = 20
            ws.column_dimensions[get_column_letter(i_col+1)].width = 12
            ws.column_dimensions[get_column_letter(i_col+2)].width = 12
            ws.column_dimensions[get_column_letter(i_col+3)].width = 12
            ws.column_dimensions[get_column_letter(i_col+4)].width = 13
            for i in range(i_col+5, i_col+12):
                ws.column_dimensions[get_column_letter(i)].width = 15

        # Activate the first sheet and save the output
        wb.active = wb.worksheets[0]
        wb.save(self.xls_output)



    @staticmethod
    def check_overlaps(fc_one, fc_two) -> bool:
        """
        STATIC CLASS METHOD

        check_overlaps: Function to compare geometries between two feature layers and determine if they overlap

        Args:
            fc_one (Feature layer): First feature layer to compare
            fc_two (Feature Layer): Second feature layer to compare

        Returns:
            bool: Flag depicting if there is overlap
        """

        # Loop through each record in the first feature layer
        with arcpy.da.SearchCursor(in_table=fc_one, field_names=['SHAPE@']) as one_cursor:
            for one_row in one_cursor:
                # Loop through each record in the second feature layer
                with arcpy.da.SearchCursor(in_table=fc_two, field_names=['SHAPE@']) as two_cursor:
                    for two_row in two_cursor:
                        # Compare the geometries to see if they intersect, if yes, then return True
                        if not one_row[0].disjoint(two_row[0]):
                            return True
                        
        # Return False if no overlaps
        return False


class CE_Value:
    """
    CLASS

    Support class object that contains all information about CE Values as pulled from the schema
    """
    def __init__(self, name:str, value_type:str, assess_year:int, unique_field: str=None, assess_field:str=None, 
                 path:str='', sql:str=None, source_field:str=None, join_table_path:str=None, 
                 join_table_field:str=None) -> None:
        """
        CLASS METHOD
        
        __init__: Initializes that class object and sets up the class properties

        Args:
            name (str): Name of the ce value dataset
            value_type (str): Provincial or Regional value
            assess_year (int): Year of assessment
            unique_field (str, optional): Unique ID field. Defaults to None.
            assess_field (str, optional): Assessment unit field. Defaults to None.
            path (str, optional): File path of the dataset. Defaults to ''.
            sql (str, optional): Definition query of the dataset. Defaults to None.
            source_field (str, optional): Source field if joining a table to this dataset. Defaults to None.
            join_table_path (str, optional): Join table path if joining to the dataset. Defaults to None.
            join_table_field (str, optional): Join field within the join table if joining. Defaults to None.
        """
        
        self.name = name
        self.value_type = value_type
        self.assessment_year = assess_year
        self.id_fields = unique_field.replace(' ','').split(',') if unique_field else []
        self.assessment_fields = assess_field.replace(' ','').split(',') if assess_field else []
        if not self.id_fields and self.assessment_fields:
            self.id_fields = self.assessment_fields
        elif not self.assessment_fields and self.id_fields:
            self.assessment_fields = self.id_fields
        self.path = path
        self.sql = sql
        self.source_field = source_field
        self.join_path = join_table_path
        self.join_field = join_table_field
        self.fc_union = None
        self.aoi = defaultdict(AOI) # Dictionary of AOI objects
        self.other_fields_schema = defaultdict(FieldSchema) # Dictionary of FieldSchema objects

class AOI:
    """ 
    CLASS
    
    Support class to contain information applicable to area of interests
    """
    def __init__(self) -> None:
        """
        CLASS METHOD

        __init__: Initializes the properties associated with the class object
        """
        self.total_area = 0
        self.assessment_units = defaultdict(Assessment_Unit) # Dictionary of Assessment_Unit objects


class Assessment_Unit:
    """
    CLASS

    Support class to contain information applicable to assessment units
    """
    def __init__(self) -> None:
        """
        CLASS METHOD

        __init__: Initializes the properties associated with the class object
        """
        self.total_area = 0
        self.aoi_area = 0
        self.au_name = ''
        self.other_fields = defaultdict()

class FieldSchema:
    """
    CLASS

    Support class to contain information applicable to field schemas
    """
    def __init__(self) -> None:
        """
        CLASS METHOD

        __init__: Initializes the properties associated with the class object
        """
        self.name = ''
        self.label = ''
        self.dict_values = defaultdict(ValueSchema) # Dictionary of ValueSchema objects
        self.value_type = ''
        self.other_fields = []

class ValueSchema:
    """
    CLASS

    Support class to contain information applicable to value schemas
    """
    def __init__(self) -> None:
        """
        CLASS METHOD

        __init__: Initializes the properties associated with the class object
        """
        self.discrete_value = ''
        self.range_high = 0
        self.range_low = 0
        self.range_operator = None
        self.label = ''
        self.style_font = Font # OpenPyXl font object
        self.style_align = Alignment # OpenPyXl alignment object
        self.style_border = Border # OpenPyXl border object
        self.style_fill = PatternFill # OpenPyXl patternfill object
        self.style_format = ''

class ExcelStyles:
    """
    CLASS

    Support class to contain information and methods applicable to OpenPyXl workbook styes
    """
    def __init__(self, wb: openpyxl.Workbook) -> None:
        """
        CLASS METHOD

        __init__: Initializes the properties associated with the class object

        Args:
            wb (openpyxl.Workbook): OpenPyXl workbook object
        """
        self.thin_border = Side(style='thin', color='000000')
        self.wb = wb

        # Create the standard styles in the workbook upon creation of the class object
        self.title = self.create_style(wb=self.wb, name='title', bold=True, font_size=12, horiz_align='left')
        self.value_header = self.create_style(wb=self.wb, name='value header', bold=True, font_size=11,
                                              cell_border=True, cell_fill='f9faed')
        self.value_subheader = self.create_style(wb=self.wb, name='value subheader', bold=True, italic=True,
                                                 cell_border=True, cell_fill='f9faed')
        self.regular = self.create_style(wb=self.wb, name='regular', cell_border=True)
        self.regular_left = self.create_style(wb=self.wb, name='regular left', cell_border=True, horiz_align='left')
        self.italics = self.create_style(wb=self.wb, name='italics', font_size=8, italic=True, horiz_align='left', 
                                         cell_border=True)
        self.percent = self.create_style(wb=self.wb, name='regular percent', cell_border=True, cell_format='##0.00%')
        self.number = self.create_style(wb=self.wb, name='regular number', horiz_align='right', cell_border=True, 
                                        cell_format='###,##0')
        self.decimal = self.create_style(wb=self.wb, name='decimal number', horiz_align='right', cell_border=True, 
                                         cell_format='###,##0.0')
        self.column_header = self.create_style(wb=self.wb, name='column header', bold=True, cell_border=True, cell_fill='edfcfc')


    def create_style(self, wb:openpyxl.Workbook, name: str, font_size: int=10, bold=False, italic=False, 
                     text_colour: str='000000', horiz_align: str='center', vert_align: str='center', 
                     wrap_text: bool=True, cell_border: bool=False, cell_fill: str=None, 
                     cell_format=None) -> NamedStyle:
        """
        CLASS METHOD

        create_style: Function that creates a style object based on the parameters passed in

        Args:
            wb (openpyxl.Workbook): OpenPyXl workbook object
            name (str): name of the style
            font_size (int, optional): font size of the cell text. Defaults to 10.
            bold (bool, optional): Is the cell bold. Defaults to False.
            italic (bool, optional): Is the cell italic. Defaults to False.
            text_colour (str, optional): Colour of the text. Defaults to '000000'.
            horiz_align (str, optional): horizontal alignment of the cell. Defaults to 'center'.
            vert_align (str, optional): vertical alignment of the cell. Defaults to 'center'.
            wrap_text (bool, optional): Is the cell text wrapped. Defaults to True.
            cell_border (bool, optional): Is there a border on the cell. Defaults to False.
            cell_fill (str, optional): Background colour of the cell. Defaults to None.
            cell_format (_type_, optional): Number format of the cell. Defaults to None.

        Returns:
            NamedStyle: OpenPyXl Named style object created from the input parameters
        """
        
        # Create the new style
        new_style = NamedStyle(name=name)
        new_style.font = Font(size=font_size, bold=bold, italic=italic, color=text_colour, name='Calibri')
        new_style.alignment = Alignment(horizontal=horiz_align, vertical=vert_align, wrap_text=wrap_text)
        if cell_border:
            new_style.border = Border(left=self.thin_border, top=self.thin_border, right=self.thin_border, 
                                      bottom=self.thin_border)
        if cell_fill:
            new_style.fill = PatternFill(patternType='solid', start_color=cell_fill)
        if cell_format:
            new_style.number_format = cell_format

        # Add the style to the workbook
        wb.add_named_style(style=new_style)

        return new_style
    
    def create_style_copy(self, wb:openpyxl.Workbook, name: str, font: Font, align: Alignment, border: Border, 
                          fill: PatternFill, num_format: str) -> NamedStyle:
        """
        CLASS METHOD

        create_style_copy: Creates a style object based on OpenPyXl style objects passed in

        Args:
            wb (openpyxl.Workbook): OpenPyXl workbook object
            name (str): name of the style
            font (Font): OpenPyXl font object
            align (Alignment): OpenPyXl alignment object
            border (Border): OpenPyXl border object
            fill (PatternFill): OpenPyXl PatternFill object
            num_format (str): Number format

        Returns:
            NamedStyle: OpenPyXl Named style object created from the input parameters
        """

        # Delete the named style if it already exists in the workbook
        if name in wb.named_styles:
            del wb._named_styles[wb.style_names.index(name)]

        # Create new style object and assign properties
        new_style = NamedStyle(name=name)
        new_style.font = font
        new_style.alignment = align
        new_style.alignment.wrap_text = True
        new_style.border = border
        new_style.fill = fill
        new_style.number_format = num_format

        # Add the style to the workbook
        wb.add_named_style(style=new_style)

        return new_style


# First line the script hits when run, passes control up to the run_app function
if __name__ == '__main__':
    run_app()
