

import unittest
import os
from csv2hdf5 import *

TEST_IN_PATH = "./j-test/db/data/tiny_file.csv"
TEST_OUT_PATH = "./j-test/db/data/tiny.h5"
TEST_META_COLS = [1, 2]

class TestSetup(unittest.TestCase):
    def test_DummyCsvWorks(self):
        # TODO: this isn't really a good test.
        try:
            pandas.read_csv(TEST_IN_PATH)
        except:
            self.fail("Couldn't read test CSV.")

    def test_BadCsvFails(self):
        dummy_file_path = "test/bad.csv"
        with self.assertRaises(Exception):
            pandas.read_csv(dummy_file_path)

class TestCountCSV(unittest.TestCase):
    def test_Rows(self):
        conv = Converter(TEST_IN_PATH, None, None)
        self.assertEqual(conv.count_rows(), 4)

    def test_Columns(self):
        conv = Converter(TEST_IN_PATH, None, None)
        self.assertEqual(conv.count_cols(), 6)

    def test_MetaCols(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        self.assertEqual(conv.count_meta_cols(), 2)
        
    def test_DataCols(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        self.assertEqual(conv.count_data_cols(), 3)

    def test_MaxMetaIx(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        self.assertEqual(conv.max_meta_ix(), 2)


class TestGetFileInfo(unittest.TestCase):
    def test_ColumnNames(self):
        conv = Converter(TEST_IN_PATH, None, None)
        COLUMNS = ['id', 'metaVal1', 'metaVal2', 'gene1', 'gene2', 'gene3']
        self.assertEqual(conv.columns(), COLUMNS)

    def test_MetaColumns(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        META_COLS = ['metaVal1', 'metaVal2']
        self.assertEqual(conv.meta_columns(), META_COLS)

    def test_MetaDict(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        META_DICT = {
            "metaVal1": {
                "blue": {0, 2},
                "yellow": {1},
                "red": {3},
            },
            "metaVal2": {
                "water": {0, 3},
                "soda": {1, 2},
            }
        }
        self.assertEqual(conv.create_meta_dict(), META_DICT)

    def test_DataNames(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        DATA_NAMES = ['gene1', 'gene2', 'gene3']
        self.assertEqual(conv.data_columns(), DATA_NAMES)

    def test_DataColIxs(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        IXS = [3, 4, 5]
        self.assertEqual(conv.data_col_ixs(), IXS)

    def testDataStartIx(self):
        conv = Converter(TEST_IN_PATH, None, TEST_META_COLS)
        self.assertEqual(conv.data_start_col(), 3)

class TestUtils(unittest.TestCase):
    def test_FileNameFromPath(self):
        conv = Converter(None, None, None)
        ABSOLUTE = "/long/file/path/name.txt"
        RELATIVE = "./path/name.txt"
        NAME = "name.txt"
        self.assertEqual(conv.file_name_from_path(ABSOLUTE), NAME)
        self.assertEqual(conv.file_name_from_path(RELATIVE), NAME)

GROUP_NAME = "tiny_file.csv"

class TestConvert(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            os.remove(TEST_OUT_PATH)
        except:
            pass
        conv = Converter(TEST_IN_PATH, TEST_OUT_PATH, TEST_META_COLS, chunk_size=1)
        conv.convert()
        cls.hdf = h5py.File(TEST_OUT_PATH, 'r')

    @classmethod
    def tearDownClass(cls):
        #os.remove(TEST_OUT_PATH)
        cls.hdf = None

    def test_FileExists(self):
        self.assertTrue(os.path.isfile(TEST_OUT_PATH))
        self.assertIsNotNone(self.__class__.hdf)
        
    def test_GroupsExist(self):
        hdf = self.__class__.hdf
        GROUPS = [u'data', u'genes', u'header', u'meta', u'samples']
        self.assertEqual(hdf[GROUP_NAME].keys(), GROUPS)

    def test_FakeGroupsDontExist(self):
        hdf = self.__class__.hdf
        with self.assertRaises(KeyError):
            hdf["bad"]
        with self.assertRaises(KeyError):
            hdf[GROUP_NAME]["bad"]

    def test_DataIsCorrect(self):
        DATA = [[ 10.1,  10.2,  10.3],
                [ 20.1,  20.2,  20.3],
                [ 30.1,  30.2,  30.3],
                [ 40.1,  40.2,  40.3]]
        data_grp = self.__class__.hdf[GROUP_NAME]["data"]
        self.assertEqual(data_grp.shape, (4, 3))
        self.assertTrue(np.allclose(data_grp.value, DATA))

    def test_GenesAreCorrect(self):
        # TODO: Change the "genes" terminology
        GENES = [u'gene1', u'gene2', u'gene3']
        genes_grp = self.__class__.hdf[GROUP_NAME]["genes"]
        self.assertEqual(genes_grp.keys(), GENES)
        for i, gene in enumerate(GENES):
            self.assertEqual(genes_grp[gene].value, i)

    def test_HeaderIsCorrect(self):
        HEADER = ['id', 'metaVal1', 'metaVal2']
        header_grp = self.__class__.hdf[GROUP_NAME]["header"]
        for i, head in enumerate(HEADER):
            self.assertEqual(header_grp.value[i], head)

    def test_MetaIsCorrect(self):
        meta = self.__class__.hdf[GROUP_NAME]["meta"]
        try:
            meta1 = meta["metaVal1"]
            meta2 = meta["metaVal2"]
            self.assertTrue(np.all(meta1["blue"].value == [0, 2]))
            self.assertTrue(np.all(meta1["red"].value == [3]))
            self.assertTrue(np.all(meta1["yellow"].value == [1]))
            self.assertTrue(np.all(meta2["soda"].value == [1, 2]))
            self.assertTrue(np.all(meta2["water"].value == [0, 3]))
        except KeyError:
            self.fail("Meaningful message")

    def test_SamplesAreCorrect(self):
        samples = self.__class__.hdf[GROUP_NAME]["samples"]
        SAMPLES = np.array([['samp1', 'blue', 'water'],
                         ['samp2', 'yellow', 'soda'],
                         ['samp3', 'blue', 'soda'],
                         ['samp4', 'red', 'water']])
        self.assertTrue(np.all(samples.value == SAMPLES))

