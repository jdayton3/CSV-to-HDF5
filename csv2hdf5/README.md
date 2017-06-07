# Geney: CSV to HDF5 Converter

##### Jonathan Dayton

##### Last updated: June 7, 2017

Note: This specification is not yet set in stone.  In particular, the vocab isn't standard across this file and the `csv2hdf5.py` file (most notably: variable ~ meta, data point ~ gene, id ~ sample)

## Overview

The `csv2hdf5` module will convert large data files into HDF5 files in order to efficiently store and query them based on variables and data points.  For example, given the following table, `table_name`:

| id | color (variable) | gender (variable) | height (data point) | weight (data point) |
|----|------------------|-------------------|---------------------|---------------------|
| s1 | blue             | m                 | 1.23                | 4.56                |
| s2 | yellow           | f                 | 2.30                | 3.06                |
| s3 | red              | m                 | 0.96                | 5.93                |

If you wanted to get the id, color, and height of all the males, a normal SQL query would be:

```sql
SELECT id, color, height
FROM table_name
WHERE gender = 'm';
```

However, with some types of data, there are up to hundreds of thousands of columns, rendering most SQL engines really inefficient...

Another thought would be to use a NoSQL database, but querying large numbers of rows in NoSQL DBs [can be bad news](https://www.joelonsoftware.com/2001/12/11/back-to-basics/). 

So we want to put `table_name` into an HDF5 file kind of like...

```js
id: [s1, s2, s3]
datapoints: [height, weight]
variables: {
    color: {
        blue: [0], yellow: [1], red: [2]
        }
    },
    gender: {
        m: [0, 2], f: [1]
    }
data: [
    [1.23, 4.56],
    [2.30, 3.06],
    [0.96, 5.93]
    ]
// the rest is inefficient storage-wise, but it means we
// don't have to traverse the variable tree repeatedly.
id_vars: [
    [blue, m],
    [yellow, f],
    [red, m]
]
// I think storing id_vars and data in separate tables is more
// efficient because HDF5 can separate floats & strings...
```

So now if I want to know which id's are for the males, I look in variables->gender->m and find that rows 0 and 2 were our males.  We can then get the 0th and 2nd values from id, id_vars, and data to reconstruct our table.  

**_Technical note: in the future, we'll want to also include an index in the HDF5 for quickly searching for all variables that contain the substring "col"_**

## Details

This will be run with a command-line interface.  Should run something like

```bash
./csv2hdf5 table_name.csv output_name.h5 "1,2"
```

Where...

- `table_name.csv` is the path to the input csv file
- `output_name.h5` is the path to the output hdf5 file
- `"1,2"` is a comma-separated list of the indices of variable (meta) columns.

_TODO: we only want to have to input the number of columns, i.e. `2` instead of `"1,2"`._

### Things to test / Future features

- Given a **chunked pandas dataframe**, can I build the correct meta tree structure?
- Can I verify that at any given time, not all of the csv is read in? (this link may be helpful: