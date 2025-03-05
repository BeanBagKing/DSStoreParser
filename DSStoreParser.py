#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# DSStoreParser
# ------------------------------------------------------
# Copyright 2019 G-C Partners, LLC
# Nicole Ibrahim
#
# G-C Partners licenses this file to you under the Apache License, Version
# 2.0 (the "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at:
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

# Modified by: Nicole Ibrahim 

import fnmatch
import csv
import sys
import os
import argparse
from time import gmtime, strftime
import datetime
import io
from ds_store_parser import ds_store_handler
from ds_store_parser.ds_store.store import codes as type_codes

__VERSION__ = "0.2.1"

folder_access_report = None
other_info_report = None
all_records_ds_store_report = None
records_parsed = 0

def get_arguments():
    """Get needed options for the cli parser interface"""
    usage = f"DSStoreParser CLI tool. v{__VERSION__}"
    usage += "\n\nSearch for .DS_Store files in the path provided and parse them."

    argument_parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=usage
    )

    argument_parser.add_argument(
        '-s',
        '--source',
        dest='source',
        action="store",
        type=str,
        required=True,
        help='The source path to search recursively for .DS_Store files to parse. '
    )
    
    argument_parser.add_argument(
        '-o',
        '--out',
        dest='outdir',
        action="store",
        type=str,
        required=True,
        help='The destination folder for generated reports.'
    )
    
    return argument_parser
    
def main():
    global folder_access_report, other_info_report, all_records_ds_store_report, records_parsed

    arguments = get_arguments()
    options = arguments.parse_args()

    s_name = '*.ds_store*'
    
    opts_source = options.source
    opts_out = options.outdir
    opts_check = False
    timestr = strftime("%Y%m%d-%H%M%S")
    
    try:
        folder_access_report = open(
            os.path.join(opts_out, f'DS_Store-Folder_Access_Report-{timestr}.tsv'),
            'w', newline='', encoding='utf-8'
        )
        other_info_report = open(
            os.path.join(opts_out, f'DS_Store-Miscellaneous_Info_Report-{timestr}.tsv'),
            'w', newline='', encoding='utf-8'
        )
        all_records_ds_store_report = open(
            os.path.join(opts_out, f'DS_Store-All_Parsed_Report-{timestr}.tsv'),
            'w', newline='', encoding='utf-8'
        )
    except Exception as exp:
        print(f'Unable to proceed. Error creating reports. Exception: {exp}')
        sys.exit(0)

    # Accounting for paths ending with \"
    if opts_source.endswith('"'):
        opts_source = opts_source[:-1]
    
    record_handler = RecordHandler(opts_check)

    for root, _, filenames in os.walk(opts_source):
        for filename in filenames:
            if fnmatch.fnmatch(filename.lower(), s_name):
                ds_file = os.path.join(root, filename)
                try:
                    with open(ds_file, "rb") as file_io:
                        try: 
                            stat_dict = record_handler.get_stats(os.lstat(ds_file))
                        except Exception as e:
                            print(f"Error stat_dict {ds_file}: {e}")
                        try:
                            parse(ds_file, file_io, stat_dict, record_handler, opts_source, opts_check)
                        except Exception as e:
                            print(f"Error parse {ds_file}: {e}")
                except Exception as e:
                    print(f"Error opening {ds_file}: {e}")

    print(f'Records Parsed: {records_parsed}')
    print(f'Reports are located in {options.outdir}')

def parse(ds_file, file_io, stat_dict, record_handler, source, opts_check):
    """Parses .DS_Store files and writes records"""
    
    ds_handler = None
    record = {'code': '', 'value': '', 'type': '', 'filename': ''}

    try:
        if stat_dict['src_size'] != 0:
            ds_handler = ds_store_handler.DsStoreHandler(file_io, ds_file)
    except Exception as exp:
        err_msg = f'ERROR: {exp} for file {ds_file}'
        print(err_msg)

    if ds_handler:
        print(f"DS_Store Found: {ds_file}")

        for rec in ds_handler:
            try:
                record_handler.write_record(rec, ds_file, source, stat_dict, opts_check)
            except Exception as e:
                print(f"Error record_handler {ds_file}: {e}")

    elif stat_dict['src_size'] == 0 and os.path.split(ds_file)[1] == '.DS_Store':
        record_handler.write_record(record, ds_file, source, stat_dict, opts_check)

def directory_recurse(file_system_path_spec, parent_path, record_handler, opts_source, opts_check):
    """Recursively searches through directories for .DS_Store files using DFVFS."""
    
    path_spec = path_spec_factory.Factory.NewPathSpec(
        file_system_path_spec.type_indicator,
        parent=file_system_path_spec.parent,
        location=parent_path
    )

    file_entry = resolver.Resolver.OpenFileEntry(path_spec)

    if file_entry is not None:
        for sub_file_entry in file_entry.sub_file_entries:
            if sub_file_entry.entry_type == 'directory': 
                dir_path = os.path.join(parent_path, sub_file_entry.name).replace("\\", "/")
                
                if dir_path.count('/') == 1:
                    print(f'Searching {dir_path} for .DS_Stores')

                new_path_spec = path_spec_factory.Factory.NewPathSpec(
                    path_spec.type_indicator,
                    parent=path_spec.parent,
                    location=dir_path
                )

                directory_recurse(new_path_spec, dir_path, record_handler, opts_source, opts_check)

            elif sub_file_entry.name.lower() == '.ds_store':
                ds_file = os.path.join(parent_path, sub_file_entry.name).replace("\\", "/")
                file_io = sub_file_entry.GetFileObject()
                
                stat_dict = {}

                setattr(file_io, 'name', ds_file)
                stats = sub_file_entry.GetStat()

                setattr(stats, 'crtime', getattr(sub_file_entry._tsk_file.info.meta, 'crtime', None))
                setattr(stats, 'ctime', getattr(sub_file_entry._tsk_file.info.meta, 'ctime', None))
                setattr(stats, 'mtime', getattr(sub_file_entry._tsk_file.info.meta, 'mtime', None))
                setattr(stats, 'atime', getattr(sub_file_entry._tsk_file.info.meta, 'atime', None))
                setattr(stats, 'mode', int(getattr(sub_file_entry._tsk_file.info.meta, 'mode', 0)))

                stat_dict = record_handler.get_stats_image(stats)

                parse(ds_file, file_io, stat_dict, record_handler, opts_source, opts_check)

            else:
                continue

def commandline_arg(bytestring):
    """Convert command line argument bytes to a string"""
    return bytestring

class RecordHandler:
    def __init__(self, opts_check):
        global folder_access_report, other_info_report, all_records_ds_store_report

        if opts_check:
            fields = [
                "generated_path",
                "record_filename",  # filename
                "record_type",      # code
                "record_format",    # type
                "record_data",      # value
                "file_exists",
                "src_create_time",
                "src_mod_time",
                "src_acc_time",
                "src_metadata_change_time",
                "src_permissions",
                "src_size",
                "block",
                "src_file"
            ]
        else:
            fields = [
                "generated_path",
                "record_filename",  # filename
                "record_type",      # code
                "record_format",    # type
                "record_data",      # value
                "src_create_time",
                "src_mod_time",
                "src_acc_time",
                "src_metadata_change_time",
                "src_permissions",
                "src_size",
                "block",
                "src_file"
            ]

        # Codes that do not always mean a folder was opened
        self.other_info_codes = {
            "Iloc", "dilc", "cmmt", "clip", "extn", "logS", "lg1S",
            "modD", "moDD", "phyS", "ph1S", "ptbL", "ptbN"
        }

        # Codes indicating folder interactions
        self.folder_interactions = {
            "dscl", "fdsc", "vSrn", "BKGD", "ICVO", "LSVO", "bwsp",
            "fwi0", "fwsw", "fwvh", "glvp", "GRP0", "icgo", "icsp",
            "icvo", "icvp", "icvt", "info", "lssp", "lsvC", "lsvo",
            "lsvt", "lsvp", "lsvP", "pict", "bRsV", "pBBk", "pBB0",
            "vstl"
        }

        self.fa_writer = csv.DictWriter(
            all_records_ds_store_report, delimiter="\t", lineterminator="\n",
            fieldnames=fields
        )
        self.fa_writer.writeheader()

        self.fc_writer = csv.DictWriter(
            folder_access_report, delimiter="\t", lineterminator="\n",
            fieldnames=fields
        )
        self.fc_writer.writeheader()

        self.oi_writer = csv.DictWriter(
            other_info_report, delimiter="\t", lineterminator="\n",
            fieldnames=fields
        )
        self.oi_writer.writeheader()

        # Rename fields for record parsing
        fields[1:5] = ["filename", "code", "type", "value"]

    def write_record(self, record, ds_file, source, stat_dict, opts_check):
        global records_parsed

        if isinstance(record, dict):
            record_dict = record
            record_dict["generated_path"] = f'EMPTY DS_STORE: {ds_file}'
            record_dict["block"] = ""
        else:
            record_dict = record.as_dict()[0]
            block = record.as_dict()[1]
            record_dict["block"] = block
            filename = record_dict["filename"]
            record_dict["generated_path"] = self.generate_fullpath(source, ds_file, filename)

            
            if opts_check:
                abs_path_to_rec_file = os.path.join(os.path.split(ds_file)[0], filename)
                if os.path.lexists(abs_path_to_rec_file):
                    record_dict["file_exists"] = "[EXISTS] NONE"
                    stat_result = self.get_stats(os.lstat(abs_path_to_rec_file))
                    if stat_result:
                        record_dict["file_exists"] = str(stat_result)
                else:
                    record_dict["file_exists"] = "[NOT EXISTS]"


            
            if record_dict["code"] == "vstl":
                record_dict["value"] = self.style_handler(record_dict)


            records_parsed += 1

        
        for key in ["value", "generated_path", "filename"]:
            try:
                record_dict[key] = record_dict[key].replace('\r', '').replace('\n', '').replace('\t', '')
            except Exception as e:
                pass
                # Error replace ./.DS_Store:11150179 'int' object has no attribute 'replace'
                # Not sure this is important? No need to replace newline/tab if it's not a string right?
                #print(f"Error replace {ds_file}:{record_dict[key]} {e}")
        

        
        record_dict["src_file"] = f'{source}, {ds_file}' if os.path.isfile(source) else ds_file
        record_dict.update({
            "src_metadata_change_time": stat_dict["src_metadata_change_time"],
            "src_acc_time": stat_dict["src_acc_time"],
            "src_mod_time": stat_dict["src_mod_time"],
            "src_create_time": stat_dict["src_birth_time"],
            "src_size": stat_dict["src_size"],
            "src_permissions": f'{stat_dict["src_perms"]}, User: {stat_dict["src_uid"]}, Group: {stat_dict["src_gid"]}'
        })

        try:
            record_dict["type"] = record_dict["type"].decode("utf-8") if isinstance(record_dict["type"], bytes) else str(record_dict["type"])
            
            if "Codec" in record_dict["type"]:
                record_dict["type"] = f'blob ({record_dict["type"]})'
        except Exception as e:
            gettype = type(record_dict["type"])
            print(f"Error here {ds_file}: type({gettype}): value: {record_dict['type']} {e}")


        check_code = record_dict["code"]
        record_dict["code"] += f" ({self.update_descriptor(record_dict)})"

        self.fa_writer.writerow(record_dict)

        if check_code in self.other_info_codes:
            self.oi_writer.writerow(record_dict)
        elif check_code in self.folder_interactions:
            self.fc_writer.writerow(record_dict)
        else:
            print(f'Code not accounted for: {record_dict["code"]}')

        

    def get_stats(self, stat_result):
        stat_dict = {
            "src_acc_time": self.convert_time(stat_result.st_atime) + " [UTC]",
            "src_mod_time": self.convert_time(stat_result.st_mtime) + " [UTC]",
            "src_perms": self.perm_to_text(stat_result.st_mode),
            "src_size": stat_result.st_size,
            "src_uid": stat_result.st_uid,
            "src_gid": stat_result.st_gid,
        }

        if hasattr(stat_result, "st_birthtime"):
            stat_dict["src_birth_time"] = self.convert_time(stat_result.st_birthtime) + " [UTC]"
        else:
            stat_dict["src_birth_time"] = self.convert_time(stat_result.st_ctime) + " [UTC]"

        stat_dict["src_metadata_change_time"] = self.convert_time(stat_result.st_ctime) + " [UTC]"
        return stat_dict

    def convert_time(self, timestamp):
        return str(datetime.datetime.fromtimestamp(timestamp, datetime.UTC))


    def perm_to_text(self, perm):
        """Converts permission mode to human-readable format."""
        perms = {
            "0": "---", "1": "--x", "2": "-w-", "3": "-wx",
            "4": "r--", "5": "r-x", "6": "rw-", "7": "rwx"
        }
        perm_oct = oct(int(perm))[-3:]
        return "Perms: {}/-{}".format(perm, "".join(perms.get(p, p) for p in perm_oct))

    def generate_fullpath(self, source, ds_file, record_filename):
        ds_store_rel_path = os.path.split(ds_file)[0] if os.path.isfile(source) else os.path.split(ds_file)[0][len(os.path.split(source)[0]):]
        generated_path = os.path.join(ds_store_rel_path, record_filename).replace('\r', '').replace('\n', '').replace('\t', '')

        if os.name == "nt":
            generated_path = generated_path.replace("\\", "/")

        return f"/{generated_path}" if not generated_path.startswith("/") else generated_path

    def update_descriptor(self, record):
        return type_codes.get(record["code"], f"Unknown Code: {record['code']}")

    def style_handler(self, record):
        styles_dict = {
            '\x00\x00\x00\x00': "0x00000000: Null",
            "none": "none: Unselected", "icnv": "icnv: Icon View",
            "clmv": "clmv: Column View", "Nlsv": "Nlsv: List View",
            "glyv": "glyv: Gallery View", "Flwv": "Flwv: CoverFlow View"
        }
        return styles_dict.get(record["value"], f"Unknown Code: {record['value']}")

if __name__ == '__main__':
    main()
