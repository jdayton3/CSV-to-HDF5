#!/usr/bin/env python

"""Converting a delimited file to hdf5 format.

Structure

"fileName" : {
    "samples" : {
        "patient1": [meta1, meta2, meta3],
        "patient2": [meta1, meta2, meta3],
        "patient3": [meta1, meta2, meta3],
    },
    "genes" : {
        "gene1": 0,
        "gene2": 1,
        "gene3": 2,
    },
    "meta" : {
        "metaType1" : {
            "var1": [0, 2], // Where these reference patients 1 & 3
            "var2": [1], // This really represents a set, which can't really be HDF5ized, I think.
        },
        "metaType2" : {
            "var1": [0, 1],
            "var2": [2],
        }
    },
    "data" : [
        [ // This represents samples[0] or patient1
            123.45, // This represents data for gene1, or genes[0]
            678.90, // gene2 or genes[1]
            135.79, // etc.
        ],
        [ 100.00, 45.25, 15.00 ], // samples[1], aka patient2
        [ 32.12, 134.24, 34.65 ],
    ]
}

"""

import argparse
import h5py
import pandas
import numpy as np

class Converter:
    """A class to convert a delimited file with meta columns to hdf5.

    Args:
        in_path (str): Path to the delimited input.
        out_path (str): Path to the hdf5 output.
        meta_cols_ixs (list): List of integer column indices for the meta columns.
        id_col (int): Column index for the row id, default 0.
        sep (str): Separation between fields in the delimited file, default ",".
    """
    def __init__(self, in_path, out_path, meta_cols_ixs, id_col=0, sep=",", chunk_size=100):
        self.in_path = in_path
        self.out_path = out_path
        self.meta_cols_ixs = meta_cols_ixs
        self.id_col = id_col
        self.sep = sep
        self.chunk_size = chunk_size
        # Create the bytes datatype so the samples dataset can contain strings
        self.bytes_type = h5py.special_dtype(vlen=bytes)

    def count_rows(self):
        # Get the line count (not including the header line)
        with open(self.in_path, "r") as in_f:
            i = -1
            for i, line in enumerate(in_f):
                pass
            return i

    def count_cols(self):
        # Get the number of columns
        return len(self.columns())

    def count_meta_cols(self):
        # Get the number of meta columns
        return max(self.meta_cols_ixs)
    
    def count_data_cols(self):
        # Get the number of data columns
        # Total number - meta cols - id col
        return self.count_cols() - self.count_meta_cols() - 1

    def max_meta_ix(self):
        return max(self.meta_cols_ixs)

    def columns(self):
        with open(self.in_path, "r") as in_f:
            header = in_f.readline().strip().split(self.sep)
            return header

    def meta_columns(self):
        return [self.columns()[i] for i in self.meta_cols_ixs]
    
    def data_columns(self):
        start_ix = self.max_meta_ix() + 1
        return self.columns()[start_ix:]

    def data_col_ixs(self):
        return range(self.max_meta_ix() + 1, self.count_cols())

    def data_start_col(self):
        return min(self.data_col_ixs())

    def create_meta_dict(self):
        meta_dict = {}
        cols = self.columns()
        for col in self.meta_cols_ixs:
            meta_dict[cols[col]] = {}
        with open(self.in_path, "r") as in_f:
            _ = in_f.readline()
            for i, line in enumerate(in_f):
                line = line.strip().split(self.sep)
                # Collect meta data. TODO: do this with the pandas df.
                for col in self.meta_cols_ixs:
                    # If this meta val isn't already in the dict, add it as a set
                    if line[col] not in meta_dict[cols[col]]:
                        meta_dict[cols[col]][line[col]] = set()
                    meta_dict[cols[col]][line[col]].add(i)
        return meta_dict

    def file_name_from_path(self, path):
        return path.split("/")[-1]

    def convert(self):
        """Convert the csv in in_path to hdf5 format in out_path

        TODO:
            add a progress bar
            make this more memory efficient (do we have to read the whole file into memory?)
        """

        num_rows = self.count_rows()

        # Open up the hdf5 file and create a new group for this file
        hdf = h5py.File(self.out_path, "a")
        group_name = self.file_name_from_path(self.in_path)
        grp = hdf.create_group(group_name)

        # Create a meta group
        meta = grp.create_group("meta")
        # And a meta dict
        meta_dict = self.create_meta_dict()
        # Add the meta_dict to the hdf5 file. TODO: clean this up
        for m_type, val_dict in meta_dict.items():
            type_group = meta.create_group(m_type)
            for m_val, samp_list in val_dict.items():
                val_data = type_group.create_dataset(m_val, (len(samp_list),), dtype='i8')
                for i, val in enumerate(samp_list):
                    val_data[i] = val

        # Put the order of the meta columns in the hdf5.
        header = self.columns()
        grp["header"] = header[:self.max_meta_ix() + 1]

        # Create a list of genes and put it in the hdf5
        genes = grp.create_group("genes")
        for i, gene in enumerate(self.data_columns()):
            genes[gene] = i

        
        # Create the samples group
        samples = grp.create_dataset("samples", (num_rows, self.count_meta_cols()+1), 
                            dtype=self.bytes_type)

        # Add the data to the hdf5 file
        data = grp.create_dataset("data", (num_rows, self.count_data_cols()))

        # Load the csv as a pandas dataframe
        gene_start_col = self.data_start_col()
        counter = 0
        df = pandas.read_csv(self.in_path, chunksize=self.chunk_size)
        while counter < num_rows:
            chunk = df.get_chunk()
            samples[counter:counter + self.chunk_size, :] = \
                            chunk.ix[:, self.id_col:self.max_meta_ix()+1]
            data[counter:counter + self.chunk_size, :] = \
                            chunk.ix[:, gene_start_col:].values
            counter += self.chunk_size




class Main:
    """Parse command line arguments and run the converter."""

    def __init__(self):
        args = self.parse_args()
        self.conv = Converter(args.inPath, args.outPath, args.metaCols,
                              id_col=args.idCol, sep=args.sep)
        self.conv.convert()

    def parse_args(self):
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(description="Convert csv to hdf5")
        parser.add_argument("inPath", type=str,
                            help="path to the input file")
        parser.add_argument("outPath", type=str,
                            help="path to the output file")
        parser.add_argument("metaCols", type=str,
                            help="list of meta columns, e.g. '1,2,3'")
        parser.add_argument("-i", "--idCol", type=int,
                            help="index for the column containing row ids, default 0")
        parser.add_argument("-s", "--sep", type=str,
                            help="separation in delimited file, default ','")
        args = parser.parse_args()
        if not args.idCol:
            args.idCol = 0
        if not args.sep:
            args.sep = ","
        args.metaCols = [int(x) for x in args.metaCols.split(args.sep)]
        return args

if __name__ == "__main__":
    Main()
